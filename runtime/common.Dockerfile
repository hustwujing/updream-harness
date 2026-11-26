ARG BOOTSTRAP_IMAGE=sandbox-harness-parallel-ali-bootstrap:build
FROM ${BOOTSTRAP_IMAGE}
USER root
ARG DEBIAN_MIRROR=http://mirrors.aliyun.com
ARG NODE_VERSION=22.14.0
ARG NODE_MIRROR=https://npmmirror.com/mirrors/node
ARG PYPI_INDEX=http://mirrors.cloud.aliyuncs.com/pypi/simple/
RUN apt-get update && apt-get install -y --no-install-recommends \
      bash build-essential ca-certificates cmake curl file git jq pkg-config procps ripgrep \
      fonts-liberation2 fonts-noto-color-emoji fonts-wqy-zenhei \
      libegl-mesa0 libegl1 libgles2 lmodern mesa-utils xauth \
      unzip xz-utils zip && rm -rf /var/lib/apt/lists/* && \
    curl -fsSL "${NODE_MIRROR}/v${NODE_VERSION}/node-v${NODE_VERSION}-linux-x64.tar.xz" \
      | tar -xJ --strip-components=1 -C /usr/local
COPY runtime/requirements-common.txt /tmp/requirements-common.txt
RUN pip install --no-cache-dir --trusted-host mirrors.cloud.aliyuncs.com \
      --index-url ${PYPI_INDEX} -r /tmp/requirements-common.txt && rm /tmp/requirements-common.txt
COPY runtime/proxy_bridge.cjs /usr/local/lib/sandbox-proxy-bridge.cjs
COPY runtime/playwright_proxy_patch.cjs /usr/local/lib/sandbox-playwright-proxy.cjs
COPY runtime/apply_patch.py /usr/local/bin/apply_patch
COPY runtime/original_compatibility_probe.py /usr/local/bin/sandbox-original-compatibility-probe
RUN chmod 0755 /usr/local/bin/apply_patch /usr/local/bin/sandbox-original-compatibility-probe && \
    chmod -R a+rX /usr/local/lib
ENV NODE_PATH=/usr/local/lib/node_modules npm_config_registry=https://registry.npmmirror.com
USER 10001:10001
