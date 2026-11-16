from pathlib import Path
import importlib.util

ROOT=Path(__file__).resolve().parents[1]
SPEC=importlib.util.spec_from_file_location("runtime_asr_cli",ROOT/"runtime/asr_cli.py")
ASR=importlib.util.module_from_spec(SPEC);SPEC.loader.exec_module(ASR)


def test_asr_timestamp_is_srt_compatible():
    assert ASR.timestamp(0)=="00:00:00,000"
    assert ASR.timestamp(3661.234)=="01:01:01,234"


def test_runtime_image_declares_offline_faster_whisper_contract():
    requirements=(ROOT/"runtime/requirements-runtime.txt").read_text()
    dockerfile=(ROOT/"runtime/base.Dockerfile").read_text()
    probe=(ROOT/"runtime/media_probe.py").read_text()
    assert "faster-whisper==1.2.1" in requirements and "ctranslate2==4.9.0" in requirements
    assert "av==16.1.0" in requirements and "onnxruntime==1.31.1" in requirements
    assert "COPY runtime/vendor/faster-whisper-small/ /opt/models/faster-whisper-small/" in dockerfile
    assert "python runtime/asr_probe.py" in dockerfile and "sandbox-transcribe" in dockerfile
    assert '"faster_whisper"' in probe and '"ctranslate2"' in probe
