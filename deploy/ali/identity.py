from pathlib import Path


ROOT=Path("/data2/wujing03/sandbox-harness-platform-parallel-ali")
BRANCH="sandbox-harness-platform-parallel-ali"
DATA_ROOT=ROOT/".data"
PORTS={"api":24767,"postgres":28433,"gateway":29882,"proxy":29883}
POSTGRES_IDENTITY="sandbox_harness_parallel_ali"
NAMESPACE="sandbox-harness-platform-parallel-ali"
REDIS_PREFIX=NAMESPACE+":"
OBJECT_PREFIX=NAMESPACE+"/"
REGISTRY_PREFIX="127.0.0.1:25001/sandbox-harness-platform-parallel-ali"
JOB_NODE_ID="ali-opensandbox"
CONTROLLED_NODE_ID="ali-controlled"

