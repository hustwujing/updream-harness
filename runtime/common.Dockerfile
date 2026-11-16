ARG ASR_SOURCE_IMAGE=sandbox-harness-runtime-base:0027ec835f16
FROM ${ASR_SOURCE_IMAGE} AS asr_source
ARG BOOTSTRAP_IMAGE=sandbox-harness-parallel-bootstrap:build
FROM ${BOOTSTRAP_IMAGE}
USER root
ARG DEBIAN_MIRROR=http://mirrors.aliyun.com
ARG NODE_VERSION=22.14.0
ARG NODE_MIRROR=https://npmmirror.com/mirrors/node
ARG PYPI_INDEX=http://mirrors.cloud.aliyuncs.com/pypi/simple/
RUN apt-get update && apt-get install -y --no-install-recommends \
      bash build-essential ca-certificates cmake curl file git jq pkg-config procps ripgrep \
      unzip xz-utils zip && rm -rf /var/lib/apt/lists/* && \
    curl -fsSL "${NODE_MIRROR}/v${NODE_VERSION}/node-v${NODE_VERSION}-linux-x64.tar.xz" \
      | tar -xJ --strip-components=1 -C /usr/local
COPY runtime/requirements-common.txt /tmp/requirements-common.txt
RUN pip install --no-cache-dir --trusted-host mirrors.cloud.aliyuncs.com \
      --index-url ${PYPI_INDEX} -r /tmp/requirements-common.txt && rm /tmp/requirements-common.txt
COPY --from=asr_source /opt/models/faster-whisper-small/ /opt/models/faster-whisper-small/
COPY --from=asr_source /opt/platform/runtime/asr-smoke.wav /opt/platform/runtime/asr-smoke.wav
COPY runtime/asr_cli.py runtime/asr_probe.py /opt/platform/runtime/
COPY runtime/proxy_bridge.cjs /usr/local/lib/sandbox-proxy-bridge.cjs
COPY runtime/playwright_proxy_patch.cjs /usr/local/lib/sandbox-playwright-proxy.cjs
COPY runtime/apply_patch.py /usr/local/bin/apply_patch
RUN printf '%s  %s\n' \
      b55496ac7940a7ae47d2c01eab40edfd8701feec1229d9cce3b40014383fb828 /opt/models/faster-whisper-small/config.json \
      3e305921506d8872816023e4c273e75d2419fb89b24da97b4fe7bce14170d671 /opt/models/faster-whisper-small/model.bin \
      fb7b63191e9bb045082c79fd742a3106a12c99513ab30df4a0d47fa6cb6fd0ab /opt/models/faster-whisper-small/tokenizer.json \
      34ce3fe1c5041027b3f8d42912270993f986dbc4bb34cf27f951e34a1e453913 /opt/models/faster-whisper-small/vocabulary.txt \
      | sha256sum -c - && \
    install -m 0755 /opt/platform/runtime/asr_cli.py /usr/local/bin/sandbox-transcribe && \
    chmod 0755 /usr/local/bin/apply_patch && chmod -R a+rX /usr/local/lib /opt/models && \
    python /opt/platform/runtime/asr_probe.py --model /opt/models/faster-whisper-small \
      --audio /opt/platform/runtime/asr-smoke.wav --expect "blue square"
ENV NODE_PATH=/usr/local/lib/node_modules npm_config_registry=https://registry.npmmirror.com
ENV SANDBOX_WHISPER_MODEL=/opt/models/faster-whisper-small
USER 10001:10001
