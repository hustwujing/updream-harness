FROM docker.m.daocloud.io/library/python:3.13-slim-bookworm@sha256:413041be39649714885528a06586416279db85744b6dcdf290af0ae557d782f6
ARG DEBIAN_MIRROR=http://mirrors.aliyun.com
ARG NODE_VERSION=22.14.0
ARG NODE_MIRROR=https://npmmirror.com/mirrors/node
ARG NPM_REGISTRY=https://registry.npmmirror.com
ARG PLAYWRIGHT_DOWNLOAD_HOST=https://npmmirror.com/mirrors/playwright
ARG PLAYWRIGHT_VERSION=1.65.0
ARG THREE_VERSION=0.187.0
ARG ESBUILD_VERSION=0.28.2
RUN sed -i -e "s|http://deb.debian.org/debian|${DEBIAN_MIRROR}/debian|g" \
      -e "s|http://security.debian.org/debian-security|${DEBIAN_MIRROR}/debian-security|g" \
      /etc/apt/sources.list.d/debian.sources && \
    apt-get update && apt-get install -y --no-install-recommends \
      build-essential \
      ca-certificates \
      cmake \
      chromium \
      curl \
      ffmpeg \
      file \
      fonts-noto-color-emoji \
      fonts-wqy-zenhei \
      git \
      imagemagick \
      jq \
      libgbm1 \
      libgl1-mesa-dri \
      libglx-mesa0 \
      mesa-utils \
      pkg-config \
      procps \
      ripgrep \
      unzip \
      xauth \
      xvfb \
      xz-utils && \
    rm -rf /var/lib/apt/lists/* && \
    curl -fsSL "${NODE_MIRROR}/v${NODE_VERSION}/node-v${NODE_VERSION}-linux-x64.tar.xz" \
      | tar -xJ --strip-components=1 -C /usr/local && \
    PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=1 npm install -g --registry="${NPM_REGISTRY}" \
      "playwright@${PLAYWRIGHT_VERSION}" "three@${THREE_VERSION}" "esbuild@${ESBUILD_VERSION}" && \
    PLAYWRIGHT_DOWNLOAD_HOST="${PLAYWRIGHT_DOWNLOAD_HOST}" \
      PLAYWRIGHT_DOWNLOAD_CONNECTION_TIMEOUT=120000 \
      PLAYWRIGHT_BROWSERS_PATH=/ms-playwright \
      playwright install chromium && \
    chmod -R a+rX /ms-playwright && \
    npm cache clean --force
ENV NODE_PATH=/usr/local/lib/node_modules
ENV npm_config_registry=https://registry.npmmirror.com
ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright
ENV CHROME_PATH=/usr/bin/chromium
ENV LIBGL_ALWAYS_SOFTWARE=1
ARG DRAWIO_VERSION=32.6.10
ARG DRAWIO_SHA256=fc96faafe68cb4e06c0b7116779891592b20fce2cde7907adec504f23c5af83c
ARG REACT_VERSION=19.3.1
ARG TYPESCRIPT_VERSION=7.0.2
ARG REACT_ICONS_VERSION=5.7.0
ARG SHARP_VERSION=0.35.5
ARG DOCX_VERSION=9.15.3
ARG PPTXGENJS_VERSION=4.0.1
ARG PNPM_VERSION=12.18.0
COPY runtime/vendor/drawio-amd64-${DRAWIO_VERSION}.deb /tmp/drawio.deb
RUN apt-get update && apt-get install -y --no-install-recommends \
      fonts-liberation2 \
      graphviz \
      libreoffice-calc \
      libreoffice-core \
      libreoffice-impress \
      libreoffice-writer \
      pandoc \
      poppler-utils \
      qpdf \
      tesseract-ocr \
      tesseract-ocr-chi-sim \
      zip && \
    echo "${DRAWIO_SHA256}  /tmp/drawio.deb" | sha256sum -c - && \
    apt-get install -y --no-install-recommends /tmp/drawio.deb && \
    rm -f /tmp/drawio.deb && \
    rm -rf /var/lib/apt/lists/*
COPY runtime/vendor/npm-prefix/lib/node_modules/ /usr/local/lib/node_modules/
COPY runtime/vendor/npm-prefix/bin/ /usr/local/bin/
RUN chmod -R a+rX /usr/local/lib/node_modules /usr/local/bin
