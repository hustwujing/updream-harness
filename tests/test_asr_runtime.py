from pathlib import Path
import importlib.util

ROOT=Path(__file__).resolve().parents[1]
SPEC=importlib.util.spec_from_file_location("runtime_asr_cli",ROOT/"runtime/asr_cli.py")
ASR=importlib.util.module_from_spec(SPEC);SPEC.loader.exec_module(ASR)


def test_asr_timestamp_is_srt_compatible():
    assert ASR.timestamp(0)=="00:00:00,000"
    assert ASR.timestamp(3661.234)=="01:01:01,234"


def test_common_image_reuses_verified_offline_asr_model():
    requirements=(ROOT/"runtime/requirements-common.txt").read_text()
    dockerfile=(ROOT/"runtime/common.Dockerfile").read_text()
    build=(ROOT/"runtime/build-images.sh").read_text()
    assert "faster-whisper==1.2.1" in requirements and "ctranslate2==4.9.0" in requirements
    assert "COPY --from=asr_source /opt/models/faster-whisper-small/" in dockerfile
    assert "sandbox-transcribe" in dockerfile and "asr_probe.py" in dockerfile
    assert "3e305921506d8872816023e4c273e75d2419fb89b24da97b4fe7bce14170d671" in dockerfile
    assert dockerfile.index("ARG BOOTSTRAP_IMAGE") < dockerfile.index("FROM ${ASR_SOURCE_IMAGE}")
    assert "SANDBOX_HARNESS_ASR_SOURCE_IMAGE" in build


def test_runtime_guidance_forbids_acoustic_guessing():
    instructions=(ROOT/"src/sandbox_runtime/instructions.py").read_text()
    assert "sandbox-transcribe SOURCE --output-dir DIRECTORY" in instructions
    assert "reverse-engineering audio, or guessing transcript content" in instructions
