import asyncio
from datetime import timedelta
from types import SimpleNamespace

from argon2 import PasswordHasher
from fastapi.testclient import TestClient
import pytest

from sandbox_harness.app import create_app
from sandbox_harness.auth import AUTH_COOKIE, AccountAuth, normalize_username, token_hash
from sandbox_harness.config import Settings
from sandbox_harness.catalog import HarnessCatalog
from sandbox_harness.domain import Harness, ThreadCreate, TurnCreate, TurnState, now
from sandbox_harness.job_contracts import JobGroupCreate, JobGroupDraftCreate, JobKind, JobRequest
from sandbox_harness.sessions import COOKIE
from sandbox_harness.store import Base, OWNER_MIGRATION_TABLES, Store


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


async def test_transparent_password_rehash_preserves_existing_sessions(tmp_path):
    store=Store('sqlite+aiosqlite:///'+str(tmp_path/'rehash.db'));await store.initialize()
    auth=AccountAuth(store,3600);password='eight888'
    weak=PasswordHasher(time_cost=1,memory_cost=8192,parallelism=1).hash(password)
    try:
        account=await store.create_user('rehash.user',weak)
        first,_=await auth.create_session(account.id);second,_=await auth.create_session(account.id)
        assert (await auth.authenticate('rehash.user',password)).id==account.id
        assert (await auth.resolve(first))[0].id==account.id
        assert (await auth.resolve(second))[0].id==account.id
    finally:await store.close()


async def test_owner_migration_catalog_covers_every_owner_table_and_moves_job_draft(tmp_path):
    owner_tables={table.name for table in Base.metadata.tables.values()
        if 'owner_id' in table.c and table.name!='users'}
    assert OWNER_MIGRATION_TABLES==owner_tables
    store=Store('sqlite+aiosqlite:///'+str(tmp_path/'owner-migration.db'));await store.initialize()
    profile=HarnessCatalog().resolve('codex','gpt-5.6','standard');source='anonymous-owner'
    try:
        thread=await store.create_thread(ThreadCreate(harness=Harness.CODEX,model='gpt-5.6',
            reasoning_level='standard',title='Migrate'),profile,source)
        turn,_,_=await store.create_turn(thread.id,TurnCreate(message='done',
            idempotency_key='migration-origin'),profile,source)
        await store.set_turn_state(turn.id,TurnState.COMPLETED)
        request=JobRequest(local_job_id='draft',kind=JobKind.OPERATION,adapter='fixture',
            descriptor_id='fixture',descriptor_version='1',descriptor_digest='a'*64,
            idempotency_key='migration-draft-job',parameters={})
        draft,_=await store.create_job_group_draft(source,JobGroupDraftCreate(
            thread_id=thread.id,origin_turn_id=turn.id,idempotency_key='migration-draft',
            jobs=(request,),expires_at=now()+timedelta(hours=1)))
        account=await store.register_user('migrated.user','password-hash',source)
        assert await store.get_job_group_draft(draft.id,account.owner_id)
        assert await store.get_job_group_draft(draft.id,source) is None
        assert (await store.get_thread(thread.id,account.owner_id)).id==thread.id
        assert await store.get_thread(thread.id,source) is None
    finally:await store.close()


async def test_registration_rejects_active_job_pending_upload_and_live_takeover(tmp_path):
    async def scenario(name,seed):
        store=Store('sqlite+aiosqlite:///'+str(tmp_path/(name+'.db')));await store.initialize()
        profile=HarnessCatalog().resolve('codex','gpt-5.6','standard');source='owner-'+name
        try:
            thread=await store.create_thread(ThreadCreate(harness=Harness.CODEX,model='gpt-5.6',
                reasoning_level='standard',title=name),profile,source)
            turn,_,_=await store.create_turn(thread.id,TurnCreate(message='done',
                idempotency_key=name+'-origin'),profile,source)
            await store.set_turn_state(turn.id,TurnState.COMPLETED)
            await seed(store,thread,turn,source)
            with pytest.raises(RuntimeError,match='active_work_prevents_registration'):
                await store.register_user(name+'.user','password-hash',source)
        finally:await store.close()
    async def active_job(store,thread,turn,source):
        request=JobRequest(local_job_id='active',kind=JobKind.OPERATION,adapter='fixture',
            descriptor_id='fixture',descriptor_version='1',descriptor_digest='a'*64,
            idempotency_key='active-job-request',parameters={})
        await store.create_job_group(source,JobGroupCreate(thread_id=thread.id,
            origin_turn_id=turn.id,idempotency_key='active-job-group',jobs=(request,)))
    async def pending_upload(store,thread,turn,source):
        await store.create_media_reference_upload(source,thread.id,SimpleNamespace(
            name='input.png',kind='image',mime='image/png',size=10,sha256='b'*64),
            object_key='pending-key',public_url='https://example.invalid/pending',
            expires_at=now()+timedelta(minutes=5))
    async def live_control(store,thread,turn,source):
        await store.begin_sandbox_control(thread.id,source,'sandbox-live','nonce',
            now()+timedelta(minutes=5))
    await scenario('active-job',active_job)
    await scenario('pending-upload',pending_upload)
    await scenario('live-control',live_control)


async def test_registration_session_insert_failure_rolls_back_account_and_owner_migration(tmp_path):
    store=Store('sqlite+aiosqlite:///'+str(tmp_path/'register-atomic.db'));await store.initialize()
    profile=HarnessCatalog().resolve('codex','gpt-5.6','standard');source='atomic-owner'
    try:
        existing=await store.create_user('existing.user','hash')
        await store.create_auth_session('f'*64,existing.id,now()+timedelta(hours=1))
        thread=await store.create_thread(ThreadCreate(harness=Harness.CODEX,model='gpt-5.6',
            reasoning_level='standard',title='Atomic'),profile,source)
        with pytest.raises(RuntimeError,match='username_taken'):
            await store.register_user('atomic.user','hash',source,
                session_token_hash='f'*64,session_expires_at=now()+timedelta(hours=1))
        assert await store.get_user_by_username('atomic.user') is None
        assert (await store.get_thread(thread.id,source)).id==thread.id
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
        unavailable=client.post('/api/auth/register',headers={'X-Updream-Ingress':'frpc'},json={
            'username':'new.frpc.user','password':'new account password'})
        assert unavailable.status_code==403 and unavailable.json()['detail']=='registration_not_available'
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
        client.app.state.broker._schedule=lambda turn_id: None
        public_turn=client.post(f"/api/threads/{thread['id']}/turns",headers={'X-Updream-Ingress':'frpc'},json={
            'message':'public network','idempotency_key':'frpc-public-network'}).json()
        assert public_turn['network_access']=='public'
        # The same account cookie resolves to the same owner on the direct ingress.
        assert [row['id'] for row in client.get('/api/threads',headers={'X-Updream-Ingress':'direct'}).json()]==[thread['id']]
        direct_thread=client.post('/api/threads',headers={'X-Updream-Ingress':'direct'},json={
            'harness':'codex','model':'gpt-5.6','reasoning_level':'standard','title':'Direct thread'}).json()
        direct_public_turn=client.post(f"/api/threads/{direct_thread['id']}/turns",headers={'X-Updream-Ingress':'direct'},json={
            'message':'account public network','idempotency_key':'direct-account-public-network'}).json()
        assert direct_public_turn['network_access']=='public'
        changed=client.post('/api/auth/change-username',headers={'X-Updream-Ingress':'frpc'},json={
            'username':'renamed.user','current_password':'initial account password'})
        assert changed.status_code==403 and changed.json()['detail']=='account_action_not_available'
        changed=client.post('/api/auth/profile',headers={'X-Updream-Ingress':'frpc'},json={
            'current_password':'initial account password','new_password':'newpass8'})
        assert changed.status_code==200 and changed.json()['user']['username']=='frpc.user'
        avatar=client.post('/api/auth/avatar',headers={'X-Updream-Ingress':'frpc'},content=b'not allowed')
        assert avatar.status_code==403 and avatar.json()['detail']=='account_action_not_available'
        logout=client.post('/api/auth/logout',headers={'X-Updream-Ingress':'frpc'});assert logout.status_code==200
        assert client.get('/api/threads',headers={'X-Updream-Ingress':'frpc'}).status_code==401
        anonymous_thread=client.post('/api/threads',headers={'X-Updream-Ingress':'direct'},json={
            'harness':'codex','model':'gpt-5.6','reasoning_level':'standard','title':'Anonymous direct'}).json()
        anonymous_turn=client.post(f"/api/threads/{anonymous_thread['id']}/turns",headers={'X-Updream-Ingress':'direct'},json={
            'message':'anonymous restricted network','idempotency_key':'direct-anonymous-restricted'}).json()
        assert anonymous_turn['network_access']=='restricted'
        relogin=client.post('/api/auth/login',headers={'X-Updream-Ingress':'frpc'},json={
            'username':'frpc.user','password':'newpass8'})
        assert relogin.status_code==200


def test_direct_registration_migrates_anonymous_threads_and_rotates_identity(tmp_path):
    database='sqlite+aiosqlite:///'+str(tmp_path/'register.db')
    settings=Settings(data_root=tmp_path,database_url=database,redis_url='',backend='fake',object_backend='local',
        model_catalog_discovery=False,session_signing_key='register-session-secret')
    with TestClient(create_app(settings)) as client:
        session=client.get('/api/session').json();assert session['ingress']=='direct' and not session['authenticated']
        thread=client.post('/api/threads',json={'harness':'codex','model':'gpt-5.6',
            'reasoning_level':'standard','title':'Anonymous work'}).json()
        registered=client.post('/api/auth/register',json={'username':'direct.user','password':'new account password'})
        assert registered.status_code==200 and registered.json()['user']['username']=='direct.user'
        assert [row['id'] for row in client.get('/api/threads').json()]==[thread['id']]
        invalid_avatar=client.post('/api/auth/avatar',content=b'not-an-image')
        assert invalid_avatar.status_code==422 and 'PNG' in invalid_avatar.json()['detail']
        png=b'\x89PNG\r\n\x1a\n'+b'avatar-bytes'
        avatar=client.post('/api/auth/avatar',content=png)
        assert avatar.status_code==200 and avatar.json()['user']['avatar_version']==1
        loaded=client.get('/api/auth/avatar');assert loaded.status_code==200 and loaded.content==png
        profile=client.post('/api/auth/profile',json={'current_password':'new account password',
            'username':'direct.renamed','new_password':'replacement password'})
        assert profile.status_code==200 and profile.json()['user']['username']=='direct.renamed'
        assert client.post('/api/auth/logout').status_code==200
        assert client.get('/api/threads').json()==[]
        login=client.post('/api/auth/login',json={'username':'direct.renamed','password':'replacement password'})
        assert login.status_code==200
        assert [row['id'] for row in client.get('/api/threads').json()]==[thread['id']]
        removed=client.delete('/api/auth/avatar');assert removed.status_code==200
        assert removed.json()['user']['avatar_version']==2 and client.get('/api/auth/avatar').status_code==404


def test_registration_response_loss_can_replay_same_credentials(tmp_path):
    settings=Settings(data_root=tmp_path,database_url='sqlite+aiosqlite:///'+str(tmp_path/'register-replay.db'),
        redis_url='',backend='fake',object_backend='local',model_catalog_discovery=False,
        session_signing_key='register-replay-secret')
    with TestClient(create_app(settings)) as client:
        client.get('/api/session');anonymous_cookie=client.cookies.get(COOKIE)
        thread=client.post('/api/threads',json={'harness':'codex','model':'gpt-5.6',
            'reasoning_level':'standard','title':'Replay'}).json()
        body={'username':'replay.user','password':'new account password'}
        first=client.post('/api/auth/register',json=body)
        assert first.status_code==200 and AUTH_COOKIE in first.cookies
        client.cookies.clear();client.cookies.set(COOKIE,anonymous_cookie)
        replay=client.post('/api/auth/register',json=body)
        assert replay.status_code==200 and replay.json()['authenticated']
        assert AUTH_COOKIE in replay.cookies
        assert [value['id'] for value in client.get('/api/threads').json()]==[thread['id']]


def test_register_with_existing_account_credentials_never_discards_current_anonymous_owner(tmp_path):
    settings=Settings(data_root=tmp_path,database_url='sqlite+aiosqlite:///'+str(tmp_path/'register-existing.db'),
        redis_url='',backend='fake',object_backend='local',model_catalog_discovery=False,
        session_signing_key='register-existing-secret')
    with TestClient(create_app(settings)) as first:
        first.get('/api/session')
        assert first.post('/api/auth/register',json={
            'username':'existing.user','password':'existing password'}).status_code==200
        assert first.post('/api/auth/logout').status_code==200
    with TestClient(create_app(settings)) as anonymous:
        anonymous.get('/api/session');anonymous_cookie=anonymous.cookies.get(COOKIE)
        thread=anonymous.post('/api/threads',json={'harness':'codex','model':'gpt-5.6',
            'reasoning_level':'standard','title':'Must remain anonymous'}).json()
        rejected=anonymous.post('/api/auth/register',json={
            'username':'existing.user','password':'existing password'})
        assert rejected.status_code==409 and rejected.json()['detail']=='username_taken'
        assert anonymous.cookies.get(COOKIE)==anonymous_cookie
        assert [value['id'] for value in anonymous.get('/api/threads').json()]==[thread['id']]


def test_registration_intent_is_bound_to_one_anonymous_owner(tmp_path):
    settings=Settings(data_root=tmp_path,database_url='sqlite+aiosqlite:///'+str(tmp_path/'register-two-owners.db'),
        redis_url='',backend='fake',object_backend='local',model_catalog_discovery=False,
        session_signing_key='register-two-owners-secret')
    with TestClient(create_app(settings)) as first:
        first.get('/api/session')
        assert first.post('/api/auth/register',json={
            'username':'shared.user','password':'shared password'}).status_code==200
    with TestClient(create_app(settings)) as second:
        second.get('/api/session')
        thread=second.post('/api/threads',json={'harness':'codex','model':'gpt-5.6',
            'reasoning_level':'standard','title':'Second owner'}).json()
        rejected=second.post('/api/auth/register',json={
            'username':'shared.user','password':'shared password'})
        assert rejected.status_code==409
        assert [value['id'] for value in second.get('/api/threads').json()]==[thread['id']]


def test_direct_registration_waits_for_active_anonymous_work(tmp_path):
    database='sqlite+aiosqlite:///'+str(tmp_path/'register-active.db')
    settings=Settings(data_root=tmp_path,database_url=database,redis_url='',backend='fake',object_backend='local',
        model_catalog_discovery=False,session_signing_key='register-active-secret')
    with TestClient(create_app(settings)) as client:
        client.get('/api/session')
        thread=client.post('/api/threads',json={'harness':'codex','model':'gpt-5.6',
            'reasoning_level':'standard','title':'Active anonymous work'}).json()
        client.app.state.broker._schedule=lambda turn_id: None
        turn=client.post(f"/api/threads/{thread['id']}/turns",json={
            'message':'still running','idempotency_key':'active-registration-turn'})
        assert turn.status_code==200
        blocked=client.post('/api/auth/register',json={
            'username':'blocked.user','password':'new account password'})
        assert blocked.status_code==409 and blocked.json()['detail']=='active_work_prevents_registration'


def test_initialize_does_not_migrate_missing_avatar_fields(tmp_path):
    import sqlite3
    database=tmp_path/'avatar-migration.db';url='sqlite+aiosqlite:///'+str(database)
    async def seed():
        store=Store(url);await store.initialize();await store.close()
    asyncio.run(seed())
    with sqlite3.connect(database) as connection:
        for column in ('avatar_object_ref','avatar_mime','avatar_version'):
            connection.execute(f'ALTER TABLE users DROP COLUMN {column}')
    async def reopen():
        store=Store(url);await store.initialize()
        await store.close()
    asyncio.run(reopen())
    with sqlite3.connect(database) as connection:
        columns={row[1] for row in connection.execute('PRAGMA table_info(users)')}
    assert not {'avatar_object_ref','avatar_mime','avatar_version'} & columns


def test_login_rate_limit_is_scoped_to_source_and_username(tmp_path):
    settings=Settings(data_root=tmp_path,database_url='sqlite+aiosqlite:///'+str(tmp_path/'rate.db'),redis_url='',
        backend='fake',object_backend='local',model_catalog_discovery=False,session_signing_key='rate-limit-secret')
    with TestClient(create_app(settings)) as client:
        for _ in range(5):
            assert client.post('/api/auth/login',headers={'X-Updream-Ingress':'frpc','X-Real-IP':'203.0.113.8'},
                json={'username':'missing.user','password':'wrong'}).status_code==401
        assert client.post('/api/auth/login',headers={'X-Updream-Ingress':'frpc','X-Real-IP':'203.0.113.8'},
            json={'username':'ｍｉｓｓｉｎｇ．ｕｓｅｒ','password':'wrong'}).status_code==429
        assert client.post('/api/auth/login',headers={'X-Updream-Ingress':'frpc','X-Real-IP':'203.0.113.8'},
            json={'username':'missing.user','password':'wrong'}).status_code==429
        assert client.post('/api/auth/login',headers={'X-Updream-Ingress':'frpc','X-Real-IP':'203.0.113.9'},
            json={'username':'missing.user','password':'wrong'}).status_code==401
