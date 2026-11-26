from sandbox_harness.common.contracts.public_errors import (
    public_job_result,
    public_turn_failure,
)
from sandbox_harness.common.contracts.domain import MediaJobView, SiteDeployJobView, TurnView
from sandbox_harness.common.contracts.job_contracts import JobView
from sandbox_harness.services.api.app import public_job_view


def test_public_job_result_removes_internal_diagnostics_and_has_stable_reference():
    internal = {
        "error_code": "provider_service_unavailable",
        "error_detail": "ReadError: https://private.invalid credential=secret",
        "exception_type": "ReadError",
        "raw_error": {"private": "body"},
        "provider_job_id": "provider-job",
    }

    first = public_job_result(internal, identity="a" * 32, origin="media-worker")
    second = public_job_result(internal, identity="a" * 32, origin="media-worker")

    assert first == second
    assert first["error_code"] == "provider_service_unavailable"
    assert first["error_message"] == "外部服务暂时不可用"
    assert first["error_guidance"]
    assert len(first["troubleshooting_id"]) == 32
    assert first["provider_job_id"] == "provider-job"
    encoded = str(first)
    assert "ReadError" not in encoded
    assert "private.invalid" not in encoded
    assert "credential" not in encoded
    assert "raw_error" not in first


def test_public_turn_failure_does_not_depend_on_raw_detail():
    first = public_turn_failure(
        code="harness_exit",
        detail="native stderr secret-one",
        turn_id="b" * 32,
    )
    second = public_turn_failure(
        code="harness_exit",
        detail="different private error",
        turn_id="b" * 32,
    )

    assert first == second
    assert first["error_code"] == "harness_exit"
    assert first["error_message"] == "执行进程异常退出，请检查已保存的文件后继续。"
    assert first["error_action"]
    assert len(first["troubleshooting_id"]) == 32
    assert "secret-one" not in str(first)


def test_public_view_serialization_excludes_internal_error_detail():
    for view in (
        TurnView.model_construct(error_code="failed",error_detail="turn secret",
            troubleshooting_id="a"*32),
        MediaJobView.model_construct(error_code="failed",error_detail="media secret",
            troubleshooting_id="b"*32),
        SiteDeployJobView.model_construct(error_code="failed",error_detail="site secret",
            troubleshooting_id="c"*32),
    ):
        payload=view.model_dump(mode="json")
        assert "error_detail" not in payload
        assert len(payload["troubleshooting_id"])==32
        assert "secret" not in str(payload)


def test_public_job_api_projection_removes_internal_result_detail():
    view=JobView.model_construct(id="d"*32,result={
        "error_code":"worker_internal_error",
        "error_detail":"RuntimeError: private path",
        "exception_type":"RuntimeError",
    })
    payload=public_job_view(view)
    assert "error_detail" not in payload["result"]
    assert "exception_type" not in payload["result"]
    assert len(payload["result"]["troubleshooting_id"])==32
