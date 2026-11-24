import importlib.util
from pathlib import Path


ROOT=Path(__file__).resolve().parents[1]


def module():
    spec=importlib.util.spec_from_file_location("aliyun_provider_real",
        ROOT/"tests/run_real_aliyun_sandbox_provider_protocol.py")
    value=importlib.util.module_from_spec(spec);spec.loader.exec_module(value);return value


def test_aliyun_real_provider_catalog_has_five_abnormal_boundaries():
    value=module()
    assert tuple(value.CASES)==("AP01","AP02","AP03","AP04","AP05")
    assert value.CASES["AP02"].__name__=="case_lost_create_response"
    assert value.CASES["AP03"].__name__=="case_external_delete"
    assert value.CASES["AP05"].__name__=="case_repeated_delete"


def test_aliyun_real_runner_requires_aliyun_backend():
    source=(ROOT/"tests/run_real_aliyun_sandbox_provider_protocol.py").read_text()
    assert "aliyun_backend_required" in source
    assert "e2b-traffic-access-token" in source
    assert "accepted Aliyun create response dropped" in source
