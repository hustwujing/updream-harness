ARG FOUNDATION_IMAGE=sandbox-harness-runtime-foundation:build
FROM ${FOUNDATION_IMAGE}
ARG PYPI_INDEX=http://mirrors.cloud.aliyuncs.com/pypi/simple/
ARG RELEASE=unknown
ENV SANDBOX_HARNESS_RELEASE=${RELEASE}
WORKDIR /opt/platform
COPY . /opt/platform
RUN pip install --no-cache-dir --trusted-host mirrors.cloud.aliyuncs.com --index-url ${PYPI_INDEX} '.[runtime-media]' && \
    pip install --no-cache-dir --no-deps runtime/skills/image-to-editable-ppt/cli && \
    python runtime/skill_bundle.py verify && \
    python runtime/toolchain_probe.py --verify runtime/toolchain.lock.json && \
    python runtime/media_probe.py && \
    install -m 0755 runtime/apply_patch.py /usr/local/bin/apply_patch && \
    install -m 0755 runtime/skill_smoke.py /usr/local/bin/sandbox-skill-smoke && \
    install -m 0755 runtime/browser_render.mjs /usr/local/bin/sandbox-browser-render && \
    install -m 0644 runtime/proxy_bridge.cjs /usr/local/lib/sandbox-proxy-bridge.cjs && \
    install -m 0644 runtime/playwright_proxy_patch.cjs /usr/local/lib/sandbox-playwright-proxy.cjs && \
    install -m 0755 runtime/chromium_proxy.cjs /usr/local/bin/chromium && \
    install -m 0755 runtime/drawio_wrapper.sh /usr/local/bin/drawio && \
    install -m 0755 runtime/browser_screenshot.mjs /usr/local/bin/sandbox-browser-screenshot && \
    find runtime/skills -type d -exec chmod 0555 {} + && \
    find runtime/skills -type f -exec chmod 0444 {} + && \
    python runtime/skill_probe.py && \
    mkdir -p /workspace/project /workspace/state /workspace/inbox /workspace/outputs /workspace/tmp /run/harness && \
    chown -R 10001:10001 /workspace /run/harness
USER 10001:10001
EXPOSE 8080
ENTRYPOINT ["sandbox-harness-runtime"]
