ARG COMMON_IMAGE=sandbox-harness-parallel-common:build
FROM ${COMMON_IMAGE}
USER root
ARG PYPI_INDEX=http://mirrors.cloud.aliyuncs.com/pypi/simple/
ARG NPM_REGISTRY=https://registry.npmmirror.com
ARG PLAYWRIGHT_DOWNLOAD_HOST=https://npmmirror.com/mirrors/playwright
ARG DRAWIO_VERSION=32.6.10
ARG DRAWIO_SHA256=fc96faafe68cb4e06c0b7116779891592b20fce2cde7907adec504f23c5af83c
COPY runtime/vendor/drawio-amd64-${DRAWIO_VERSION}.deb /tmp/drawio.deb
RUN apt-get update && apt-get install -y --no-install-recommends chromium dvisvgm ffmpeg graphviz \
      imagemagick libcairo2-dev libpango1.0-dev libreoffice-calc libreoffice-core \
      libreoffice-impress libreoffice-writer pandoc poppler-utils qpdf tesseract-ocr \
      tesseract-ocr-chi-sim texlive-fonts-recommended texlive-latex-base texlive-latex-extra xvfb && \
    echo "${DRAWIO_SHA256}  /tmp/drawio.deb" | sha256sum -c - && \
    apt-get install -y --no-install-recommends /tmp/drawio.deb && rm /tmp/drawio.deb && \
    rm -rf /var/lib/apt/lists/*
COPY runtime/requirements-runtime.txt /tmp/requirements-profile.txt
RUN pip install --no-cache-dir --trusted-host mirrors.cloud.aliyuncs.com \
      --index-url ${PYPI_INDEX} -r /tmp/requirements-profile.txt && rm /tmp/requirements-profile.txt && \
    apt-get purge -y libcairo2-dev libpango1.0-dev && apt-get autoremove -y && \
    PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=1 npm install -g --force --registry="${NPM_REGISTRY}" \
      playwright@1.65.0 three@0.187.0 esbuild@0.28.2 react@19.3.1 react-dom@19.3.1 \
      react-icons@5.7.0 typescript@7.0.2 pnpm@12.18.0 docx@9.15.3 \
      pptxgenjs@4.0.1 sharp@0.35.5 && \
    PLAYWRIGHT_DOWNLOAD_HOST="${PLAYWRIGHT_DOWNLOAD_HOST}" PLAYWRIGHT_BROWSERS_PATH=/ms-playwright \
      playwright install chromium && chmod -R a+rX /ms-playwright && npm cache clean --force && \
    for command in sandbox-job sandbox-artifact sandbox-review sandbox-media sandbox-site sandbox-background sandbox-interact; do \
      ln -s /usr/local/bin/sandbox-runtime-cli "/usr/local/bin/$command"; done
RUN ln -s /usr/local/bin/sandbox-runtime-cli /usr/local/bin/editppt
COPY runtime/browser_screenshot.mjs /usr/local/bin/sandbox-browser-screenshot
COPY runtime/browser_render.mjs /usr/local/bin/sandbox-browser-render
COPY runtime/chromium_proxy.cjs /usr/local/bin/chromium
COPY runtime/drawio_wrapper.sh /usr/local/bin/drawio
RUN chmod 0755 /usr/local/bin/sandbox-browser-screenshot /usr/local/bin/sandbox-browser-render \
      /usr/local/bin/chromium /usr/local/bin/drawio
ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright CHROME_PATH=/usr/bin/chromium
USER 10001:10001
