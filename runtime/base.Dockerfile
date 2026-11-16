ARG FOUNDATION_IMAGE=sandbox-harness-runtime-foundation:build
FROM ${FOUNDATION_IMAGE}
ARG PYPI_INDEX=http://mirrors.cloud.aliyuncs.com/pypi/simple/
WORKDIR /opt/platform
COPY runtime/requirements-runtime.txt /tmp/requirements-runtime.txt
RUN apt-get update && apt-get install -y --no-install-recommends libcairo2-dev libpango1.0-dev && \
    pip install --no-cache-dir --trusted-host mirrors.cloud.aliyuncs.com --index-url ${PYPI_INDEX} \
      -r /tmp/requirements-runtime.txt && \
    apt-get purge -y libcairo2-dev libpango1.0-dev && \
    apt-get autoremove -y && rm -rf /var/lib/apt/lists/*
COPY runtime/requirements-runtime.txt /opt/bootstrap/requirements-runtime.txt
COPY runtime/vendor/faster-whisper-small/ /opt/models/faster-whisper-small/
ARG SANDBOX_USERNAME=updream
COPY src/sandbox_bootstrap /opt/bootstrap/sandbox_bootstrap
COPY runtime/bundle_public_key.pem /opt/bootstrap/bundle_public_key.pem
COPY runtime/*.py runtime/*.mjs runtime/*.cjs runtime/*.sh runtime/*.json /opt/platform/runtime/
RUN python runtime/toolchain_probe.py --verify runtime/toolchain.lock.json && \
    python runtime/media_probe.py && \
    install -m 0755 runtime/apply_patch.py /usr/local/bin/apply_patch && \
    install -m 0755 runtime/manim_smoke.py /usr/local/bin/sandbox-manim-smoke && \
    install -m 0755 runtime/asr_cli.py /usr/local/bin/sandbox-transcribe && \
    install -m 0755 runtime/skill_smoke.py /usr/local/bin/sandbox-skill-smoke && \
    install -m 0755 runtime/browser_render.mjs /usr/local/bin/sandbox-browser-render && \
    install -m 0644 runtime/proxy_bridge.cjs /usr/local/lib/sandbox-proxy-bridge.cjs && \
    install -m 0644 runtime/playwright_proxy_patch.cjs /usr/local/lib/sandbox-playwright-proxy.cjs && \
    install -m 0755 runtime/chromium_proxy.cjs /usr/local/bin/chromium && \
    install -m 0755 runtime/drawio_wrapper.sh /usr/local/bin/drawio && \
    install -m 0755 runtime/bootstrap_entrypoint.sh /usr/local/bin/sandbox-harness-runtime && \
    for command in sandbox-media sandbox-site sandbox-background sandbox-interact editppt; do \
      install -m 0755 runtime/runtime_cli_dispatch.sh "/usr/local/bin/$command"; \
    done && \
    install -m 0755 runtime/browser_screenshot.mjs /usr/local/bin/sandbox-browser-screenshot && \
    chmod -R a+rX runtime /opt/bootstrap && \
    chmod -R a+rX /opt/models/faster-whisper-small && \
    python runtime/asr_probe.py --model /opt/models/faster-whisper-small \
      --audio runtime/asr-smoke.wav --expect "blue square" && \
    echo "${SANDBOX_USERNAME}" | grep -Eq '^[a-z_][a-z0-9_-]{1,31}$' && \
    groupadd --gid 10001 "${SANDBOX_USERNAME}" && \
    useradd --uid 10001 --gid 10001 --home-dir /workspace/state/home --no-create-home \
      --shell /bin/bash "${SANDBOX_USERNAME}" && \
    mkdir -p /workspace/project /workspace/state/home /workspace/inbox /workspace/outputs /workspace/tmp /run/harness && \
    chown -R 10001:10001 /workspace /run/harness
ENV USER=${SANDBOX_USERNAME}
ENV LOGNAME=${SANDBOX_USERNAME}
ENV PYTHONPATH=/opt/bootstrap
ARG RELEASE=unknown
ENV SANDBOX_HARNESS_RELEASE=${RELEASE}
ENV SANDBOX_WHISPER_MODEL=/opt/models/faster-whisper-small
USER 10001:10001
EXPOSE 8080
ENTRYPOINT ["python", "-m", "sandbox_bootstrap.main"]
