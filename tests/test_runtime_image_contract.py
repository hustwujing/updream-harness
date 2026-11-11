from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_foundation_installs_declared_browser_and_image_tools_from_domestic_mirrors():
    source = (ROOT / "runtime/foundation.Dockerfile").read_text()
    assert "ARG DEBIAN_MIRROR=http://mirrors.aliyun.com" in source
    assert "ARG NPM_REGISTRY=https://registry.npmmirror.com" in source
    assert "ARG PLAYWRIGHT_DOWNLOAD_HOST=https://npmmirror.com/mirrors/playwright" in source
    assert "imagemagick" in source
    assert "playwright install chromium" in source
    assert "PLAYWRIGHT_BROWSERS_PATH=/ms-playwright" in source


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
    assert (ROOT / "runtime/apply_patch.py").is_file()
    assert '"beautifulsoup4>=4.12,<5"' in dependencies
    assert '"psutil>=6,<8"' in dependencies
    assert '"bs4"' in probe
    assert '"psutil"' in probe
