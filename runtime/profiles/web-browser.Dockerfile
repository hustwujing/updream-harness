ARG COMMON_IMAGE=sandbox-harness-parallel-common:build
FROM ${COMMON_IMAGE}
USER root
ARG NPM_REGISTRY=https://registry.npmmirror.com
ARG PLAYWRIGHT_DOWNLOAD_HOST=https://npmmirror.com/mirrors/playwright
RUN apt-get update && apt-get install -y --no-install-recommends chromium libgbm1 \
      libgl1-mesa-dri libglx-mesa0 fonts-noto-color-emoji fonts-wqy-zenhei && \
    rm -rf /var/lib/apt/lists/* && \
    PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=1 npm install -g --registry="${NPM_REGISTRY}" \
      playwright@1.65.0 three@0.187.0 esbuild@0.28.2 react@19.3.1 \
      react-dom@19.3.1 typescript@7.0.2 pnpm@12.18.0 && \
    PLAYWRIGHT_DOWNLOAD_HOST="${PLAYWRIGHT_DOWNLOAD_HOST}" PLAYWRIGHT_BROWSERS_PATH=/ms-playwright \
      playwright install chromium && chmod -R a+rX /ms-playwright && npm cache clean --force
COPY runtime/requirements-web.txt /tmp/requirements-profile.txt
COPY runtime/browser_screenshot.mjs /usr/local/bin/sandbox-browser-screenshot
COPY runtime/browser_render.mjs /usr/local/bin/sandbox-browser-render
COPY runtime/chromium_proxy.cjs /usr/local/bin/chromium
RUN pip install --no-cache-dir --trusted-host mirrors.cloud.aliyuncs.com \
      --index-url http://mirrors.cloud.aliyuncs.com/pypi/simple/ -r /tmp/requirements-profile.txt && \
    rm /tmp/requirements-profile.txt && chmod 0755 /usr/local/bin/sandbox-browser-screenshot \
      /usr/local/bin/sandbox-browser-render /usr/local/bin/chromium && \
    for command in sandbox-job sandbox-artifact sandbox-review sandbox-background sandbox-interact; do \
      ln -s /usr/local/bin/sandbox-runtime-cli "/usr/local/bin/$command"; done
ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright CHROME_PATH=/usr/bin/chromium
USER 10001:10001
