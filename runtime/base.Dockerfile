ARG FOUNDATION_IMAGE=sandbox-harness-runtime-foundation:build
FROM ${FOUNDATION_IMAGE}
ARG PYPI_INDEX=http://mirrors.cloud.aliyuncs.com/pypi/simple/
WORKDIR /opt/platform
COPY . /opt/platform
RUN pip install --no-cache-dir --trusted-host mirrors.cloud.aliyuncs.com --index-url ${PYPI_INDEX} . && \
    python runtime/toolchain_probe.py --verify runtime/toolchain.lock.json && \
    install -m 0755 runtime/browser_render.mjs /usr/local/bin/sandbox-browser-render && \
    mkdir -p /workspace/project /workspace/state /workspace/inbox /workspace/outputs /workspace/tmp /run/harness && \
    chown -R 10001:10001 /workspace /run/harness
USER 10001:10001
EXPOSE 8080
ENTRYPOINT ["sandbox-harness-runtime"]
