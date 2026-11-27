from pathlib import Path


ROOT=Path("/data/wujing02/sandbox-harness-platform-parallel-ali")
BRANCH="sandbox-harness-platform-parallel-ali"
DATA_ROOT=ROOT/".data"
PORTS={"api":24767,"postgres":28433,"gateway":29882,"proxy":29883}
POSTGRES_IDENTITY="sandbox_harness_parallel_ali"
NAMESPACE="sandbox-harness-platform-parallel-ali-wujing02"
REDIS_PREFIX=NAMESPACE+":"
OBJECT_PREFIX=NAMESPACE+"/"
MEDIA_REFERENCE_PREFIX="aniforge/wujing02"
REGISTRY_PREFIX="127.0.0.1:25001/sandbox-harness-platform-parallel-ali-wujing02"
JOB_NODE_ID="ali-opensandbox-wujing02"
CONTROLLED_NODE_ID="ali-controlled-wujing02"
