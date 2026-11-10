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
    assert helper.is_file()
    text = helper.read_text()
    assert "proxy: proxyBridge.value" in text
    assert "HTTPS_PROXY" in text
    assert "Proxy-Authorization: ${authorization}" in text
    assert "server.listen(0, '127.0.0.1'" in text
    assert "for (const socket of sockets) socket.destroy()" in text
