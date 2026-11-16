from pathlib import Path
import json
import subprocess
import sys

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
import pytest


ROOT = Path(__file__).resolve().parents[1]


def test_foundation_installs_declared_browser_and_image_tools_from_domestic_mirrors():
    source = (ROOT / "runtime/foundation.Dockerfile").read_text()
    assert "ARG DEBIAN_MIRROR=http://mirrors.aliyun.com" in source
    assert "ARG NPM_REGISTRY=https://registry.npmmirror.com" in source
    assert "ARG PLAYWRIGHT_DOWNLOAD_HOST=https://npmmirror.com/mirrors/playwright" in source
    assert "imagemagick" in source
    assert "dvisvgm" in source and "texlive-latex-extra" in source
    assert "playwright install chromium" in source
    assert "PLAYWRIGHT_BROWSERS_PATH=/ms-playwright" in source


def test_runtime_user_is_named_and_keeps_stable_uid_contract():
    source=(ROOT/"runtime/base.Dockerfile").read_text()
    build=(ROOT/"runtime/build-images.sh").read_text()
    assert "ARG SANDBOX_USERNAME=updream" in source
    assert 'useradd --uid 10001 --gid 10001' in source
    assert 'SANDBOX_HARNESS_SANDBOX_USERNAME:-updream' in build


def test_runtime_python_dependencies_are_a_stable_layer_before_application_source():
    source = (ROOT / "runtime/base.Dockerfile").read_text()
    requirements = ROOT / "runtime/requirements-runtime.txt"
    assert requirements.is_file() and "opencv-python-headless==" in requirements.read_text()
    assert source.index("COPY runtime/requirements-runtime.txt") < source.index("COPY src")
    assert source.index("-r /tmp/requirements-runtime.txt") < source.index("COPY src")
    assert "COPY . /opt/platform" not in source
    assert "COPY src /opt/platform/src" not in source
    assert "ENTRYPOINT [\"python\", \"-m\", \"sandbox_bootstrap.main\"]" in source
    assert "bootstrap_entrypoint.sh /usr/local/bin/sandbox-harness-runtime" in source
    assert 'runtime_cli_dispatch.sh "/usr/local/bin/$command"' in source
    assert source.index("COPY src/sandbox_bootstrap") < source.index("ARG RELEASE=unknown")


def test_runtime_installs_supported_public_browser_helper():
    source = (ROOT / "runtime/base.Dockerfile").read_text()
    helper = ROOT / "runtime/browser_screenshot.mjs"
    assert "runtime/browser_screenshot.mjs /usr/local/bin/sandbox-browser-screenshot" in source
    assert "runtime/chromium_proxy.cjs /usr/local/bin/chromium" in source
    assert "runtime/playwright_proxy_patch.cjs /usr/local/lib/sandbox-playwright-proxy.cjs" in source
    assert "runtime/proxy_bridge.cjs /usr/local/lib/sandbox-proxy-bridge.cjs" in source
    assert helper.is_file()
    text = helper.read_text()
    assert "proxy: proxyBridge.value" in text
    bridge = (ROOT / "runtime/proxy_bridge.cjs").read_text()
    assert "HTTPS_PROXY" in bridge
    assert "Proxy-Authorization: ${authorization}" in bridge
    assert "server.listen(0, '127.0.0.1'" in bridge
    assert "for (const socket of sockets) socket.destroy()" in bridge
    patch = (ROOT / "runtime/playwright_proxy_patch.cjs").read_text()
    assert "patchBrowserType(exported?.chromium)" in patch


def test_runtime_installs_patch_helper_and_probes_recovered_dependencies():
    dockerfile = (ROOT / "runtime/base.Dockerfile").read_text()
    probe = (ROOT / "runtime/media_probe.py").read_text()
    dependencies = (ROOT / "pyproject.toml").read_text()
    assert "runtime/apply_patch.py /usr/local/bin/apply_patch" in dockerfile
    assert "runtime/manim_smoke.py /usr/local/bin/sandbox-manim-smoke" in dockerfile
    assert (ROOT / "runtime/apply_patch.py").is_file()
    assert '"beautifulsoup4>=4.12,<5"' in dependencies
    assert '"psutil>=6,<8"' in dependencies
    assert '"bs4"' in probe
    assert '"psutil"' in probe
    assert '"manim"' in probe and '"manimpango"' in probe


def test_parallel_runtime_uses_four_stable_layers_and_domestic_mirrors():
    bootstrap=(ROOT/"runtime/bootstrap.Dockerfile").read_text()
    common=(ROOT/"runtime/common.Dockerfile").read_text()
    assert "docker.m.daocloud.io/library/python" in bootstrap
    assert "ARG DEBIAN_MIRROR=http://mirrors.aliyun.com" in bootstrap
    assert "USER 10001:10001" in bootstrap and 'ENTRYPOINT ["/usr/bin/tini"' in bootstrap
    assert "HEALTHCHECK" in bootstrap
    assert "skills" not in bootstrap.lower() and "ANTHROPIC_API_KEY" not in bootstrap
    assert "ARG NODE_MIRROR=https://npmmirror.com/mirrors/node" in common
    assert "ARG PYPI_INDEX=http://mirrors.cloud.aliyuncs.com/pypi/simple/" in common
    assert "COPY src/sandbox_runtime" not in common and "requirements-media" not in common


def test_parallel_common_layer_covers_bootstrap_and_dynamic_runtime_imports():
    requirements=(ROOT/"runtime/requirements-common.txt").read_text()
    for dependency in ("cryptography","fastapi","httpx","psutil","pydantic","uvicorn"):
        assert dependency+"==" in requirements
    bootstrap="\n".join(path.read_text() for path in (ROOT/"src/sandbox_bootstrap").glob("*.py"))
    runtime="\n".join(path.read_text() for path in (ROOT/"src/sandbox_runtime").glob("*.py"))
    assert "from cryptography" in bootstrap and "from cryptography" in runtime


def test_parallel_profiles_are_dependency_only_and_cover_declared_catalog():
    catalog=json.loads((ROOT/"deploy/profile-catalog.example.json").read_text())
    expected={row["id"] for row in catalog["runtime_dependency_profiles"]}
    dockerfiles={path.stem for path in (ROOT/"runtime/profiles").glob("*.Dockerfile")}
    assert dockerfiles==expected=={"primary-full-cpu","general-cpu","media-cpu","office",
        "web-browser","data-analysis"}
    combined="\n".join((ROOT/"runtime/profiles"/f"{name}.Dockerfile").read_text()
        for name in sorted(expected))
    assert "COPY src/" not in combined and "OPENAI_API_KEY" not in combined
    assert "ANTHROPIC_API_KEY" not in combined and "runtime/skills" not in combined
    assert "ffmpeg" in (ROOT/"runtime/profiles/media-cpu.Dockerfile").read_text()
    assert "libreoffice" in (ROOT/"runtime/profiles/office.Dockerfile").read_text()
    assert "playwright install chromium" in (ROOT/"runtime/profiles/web-browser.Dockerfile").read_text()
    assert "sqlglot" in (ROOT/"runtime/requirements-data.txt").read_text()


@pytest.mark.parametrize("harness,marker",[("codex","@openai/codex"),
    ("claude","@anthropic-ai/claude-code"),("deepseek","deepseek-harness-sdk")])
def test_parallel_harness_layer_is_thin(harness,marker):
    source=(ROOT/f"runtime/{harness}.Dockerfile").read_text()
    assert "ARG PROFILE_IMAGE=" in source and "FROM ${PROFILE_IMAGE}" in source
    assert marker in source
    assert "apt-get" not in source and "requirements-runtime" not in source
    assert "runtime/skills" not in source and "COPY src/" not in source


def test_selective_build_keeps_state_under_data2_and_reuses_original_vendor():
    source=(ROOT/"runtime/build-images.sh").read_text()
    assert 'output_root="$root/.data/image-build/$tag"' in source
    assert "/data2/wujing03/sandbox-harness-platform/runtime/vendor" in source
    assert '--build-context "drawio_vendor=$drawio_vendor"' in source
    assert "partial development matrix" in source
    assert "BUILD_PROFILES" in source and "BUILD_HARNESSES" in source


def test_profile_catalog_builder_requires_complete_digest_pinned_matrix(tmp_path):
    private=Ed25519PrivateKey.generate(); key=tmp_path/"key.pem"
    key.write_bytes(private.private_bytes(serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,serialization.NoEncryption()))
    template=json.loads((ROOT/"deploy/profile-catalog.example.json").read_text())
    rows=[]
    for profile in template["runtime_dependency_profiles"]:
        for harness in profile["supported_harnesses"]:
            digest=(profile["id"]+":"+harness).encode().hex()[:64].ljust(64,"0")
            rows.append(f"{profile['id']}\t{harness}\tregistry/{profile['id']}-{harness}@sha256:{digest}")
    matrix=tmp_path/"images.tsv";matrix.write_text("\n".join(rows)+"\n")
    output=tmp_path/"catalog.json"
    subprocess.run([sys.executable,str(ROOT/"runtime/build_profile_catalog.py"),
        "--matrix",str(matrix),"--template",str(ROOT/"deploy/profile-catalog.example.json"),
        "--output",str(output),"--signing-key",str(key)],check=True)
    built=json.loads(output.read_text())
    assert output.with_suffix(".json.sig").is_file()
    assert all(set(row["image_refs"])==set(row["supported_harnesses"])
        for row in built["runtime_dependency_profiles"])
    matrix.write_text("\n".join(rows[:-1])+"\n")
    failure=subprocess.run([sys.executable,str(ROOT/"runtime/build_profile_catalog.py"),
        "--matrix",str(matrix),"--template",str(ROOT/"deploy/profile-catalog.example.json"),
        "--output",str(output),"--signing-key",str(key)],capture_output=True,text=True)
    assert failure.returncode!=0 and "incomplete" in failure.stderr
