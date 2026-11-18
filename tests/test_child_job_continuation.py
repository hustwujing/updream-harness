from sandbox_harness.job_contracts import JobGroupCreate,JobKind,JobRequest,JobState
from sandbox_harness.store import Store
from sandbox_harness.job_continuation import ContinuationDispatcher


def request(identity,kind=JobKind.OPERATION):
    return JobRequest(local_job_id=identity,kind=kind,adapter='fake',descriptor_id='fake',
        descriptor_version='1',descriptor_digest='a'*64,idempotency_key=identity+'-request')


async def test_nested_group_binds_results_to_parent_job(tmp_path):
    store=Store('sqlite+aiosqlite:///'+str(tmp_path/'child-continuation.db'));await store.initialize()
    try:
        parent_group,parent_jobs=await store.create_job_group('owner',JobGroupCreate(
            thread_id='b'*32,origin_turn_id='c'*32,idempotency_key='parent-group',
            jobs=[request('parent',JobKind.HARNESS)]))
        nested,_=await store.create_job_group('owner',JobGroupCreate(
            thread_id='b'*32,origin_turn_id='c'*32,idempotency_key='nested-group',
            jobs=[request('segment')],authorization_snapshot={
                'parent_job_id':parent_jobs[0].id,'delegation_depth':1}))
        assert await store.child_parent_for_group(nested.id)==parent_jobs[0].id
    finally:await store.close()


async def test_nested_outbox_targets_child_without_primary_turn():
    class Store:
        ready=[];finished=[]
        async def claim_job_outbox_batch(self,*args,**kwargs):return [{
            'outbox_id':'o'*32,'group_id':'g'*32,'thread_id':'t'*32,
            'result_digest':'d'*64,'results':[{'state':'SUCCEEDED'}],
            'batch_digest':'b'*64}]
        async def child_parent_for_group(self,group_id):return 'p'*32
        async def ready_child_continuation(self,*args):self.ready.append(args)
        async def finish_job_outbox_batch(self,*args,**kwargs):self.finished.append((args,kwargs))
    class Broker:
        async def submit(self,*args,**kwargs):raise AssertionError('must not resume Primary')
    store=Store();count=await ContinuationDispatcher(store,Broker()).run_once()
    assert count==1 and store.ready[0][0]=='p'*32
    assert store.finished[0][1]['continuation_turn_id']=='p'*32
