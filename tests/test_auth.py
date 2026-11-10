import asyncio

from argon2 import PasswordHasher
from fastapi.testclient import TestClient
import pytest

from sandbox_harness.app import create_app
from sandbox_harness.auth import AUTH_COOKIE, AccountAuth, normalize_username, token_hash
from sandbox_harness.config import Settings
from sandbox_harness.sessions import COOKIE
from sandbox_harness.store import Store


async def test_account_password_sessions_and_rotation(tmp_path):
    store=Store('sqlite+aiosqlite:///'+str(tmp_path/'auth-store.db'));await store.initialize();auth=AccountAuth(store,3600)
    try:
        password='eight888'
        account=await store.create_user(normalize_username('Test.User'),await auth.hash_password(password))
        with pytest.raises(ValueError):await auth.hash_password('short7')
        assert (await auth.authenticate('TEST.USER',password)).id==account.id
        assert await auth.authenticate('test.user','wrong password') is None
        first,first_hash=await auth.create_session(account.id);second,second_hash=await auth.create_session(account.id)
        assert (await auth.resolve(first))[0].id==account.id and (await auth.resolve(second))[0].id==account.id
        renamed=await store.update_username(account.id,'renamed.user');assert renamed.owner_id==account.owner_id
        await store.update_password(account.id,await auth.hash_password('new password for this account'),keep_token_hash=second_hash)
        assert (await auth.resolve(first))[0] is None
        assert (await auth.resolve(second))[0].id==account.id
        assert await auth.authenticate('renamed.user','new password for this account')
        await store.set_user_disabled(account.id,True);assert (await auth.resolve(second))[0] is None
    finally:await store.close()


def test_frpc_requires_account_while_direct_ingress_remains_anonymous(tmp_path):
    database='sqlite+aiosqlite:///'+str(tmp_path/'auth-api.db')
    async def seed():
        store=Store(database);await store.initialize()
        try:await store.create_user('frpc.user',PasswordHasher(time_cost=3,memory_cost=65536,parallelism=2).hash('initial account password'))
        finally:await store.close()
    asyncio.run(seed())
    settings=Settings(data_root=tmp_path,database_url=database,redis_url='',backend='fake',object_backend='local',
        model_catalog_discovery=False,session_signing_key='account-api-session-secret')
    with TestClient(create_app(settings)) as client:
        direct=client.get('/api/session')
        assert direct.json()['authentication_required'] is False
        assert COOKIE in direct.cookies
        client.cookies.clear()
        required=client.get('/api/session',headers={'X-Updream-Ingress':'frpc'})
        assert required.json()=={'ready':True,'ingress':'frpc','authentication_required':True,'authenticated':False,'user':None}
        assert COOKIE not in required.cookies
        blocked=client.get('/api/threads',headers={'X-Updream-Ingress':'frpc'})
        assert blocked.status_code==401 and blocked.json()['detail']=='authentication_required'
        failed=client.post('/api/auth/login',headers={'X-Updream-Ingress':'frpc'},json={'username':'frpc.user','password':'wrong'})
        assert failed.status_code==401 and AUTH_COOKIE not in failed.cookies
        login=client.post('/api/auth/login',headers={'X-Updream-Ingress':'frpc'},json={'username':'FRPC.USER','password':'initial account password'})
        assert login.status_code==200 and login.json()['user']['username']=='frpc.user' and AUTH_COOKIE in login.cookies
        authenticated=client.get('/api/session',headers={'X-Updream-Ingress':'frpc'}).json()
        assert authenticated['authenticated'] and not authenticated['authentication_required']
        thread=client.post('/api/threads',headers={'X-Updream-Ingress':'frpc'},json={
            'harness':'codex','model':'gpt-5.6','reasoning_level':'standard','title':'Account thread'}).json()
        # The same account cookie resolves to the same owner on the direct ingress.
        assert [row['id'] for row in client.get('/api/threads',headers={'X-Updream-Ingress':'direct'}).json()]==[thread['id']]
        changed=client.post('/api/auth/change-username',headers={'X-Updream-Ingress':'frpc'},json={
            'username':'renamed.user','current_password':'initial account password'})
        assert changed.status_code==200 and changed.json()['user']['username']=='renamed.user'
        changed=client.post('/api/auth/change-password',headers={'X-Updream-Ingress':'frpc'},json={
            'current_password':'initial account password','new_password':'newpass8'})
        assert changed.status_code==200
        logout=client.post('/api/auth/logout',headers={'X-Updream-Ingress':'frpc'});assert logout.status_code==200
        assert client.get('/api/threads',headers={'X-Updream-Ingress':'frpc'}).status_code==401
        relogin=client.post('/api/auth/login',headers={'X-Updream-Ingress':'frpc'},json={
            'username':'renamed.user','password':'newpass8'})
        assert relogin.status_code==200


def test_login_rate_limit_is_scoped_to_source_and_username(tmp_path):
    settings=Settings(data_root=tmp_path,database_url='sqlite+aiosqlite:///'+str(tmp_path/'rate.db'),redis_url='',
        backend='fake',object_backend='local',model_catalog_discovery=False,session_signing_key='rate-limit-secret')
    with TestClient(create_app(settings)) as client:
        for _ in range(5):
            assert client.post('/api/auth/login',headers={'X-Updream-Ingress':'frpc','X-Real-IP':'203.0.113.8'},
                json={'username':'missing.user','password':'wrong'}).status_code==401
        assert client.post('/api/auth/login',headers={'X-Updream-Ingress':'frpc','X-Real-IP':'203.0.113.8'},
            json={'username':'missing.user','password':'wrong'}).status_code==429
        assert client.post('/api/auth/login',headers={'X-Updream-Ingress':'frpc','X-Real-IP':'203.0.113.9'},
            json={'username':'missing.user','password':'wrong'}).status_code==401
