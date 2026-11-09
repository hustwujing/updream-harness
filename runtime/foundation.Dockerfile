FROM docker.m.daocloud.io/library/python:3.13-slim-bookworm@sha256:413041be39649714885528a06586416279db85744b6dcdf290af0ae557d782f6
ARG DEBIAN_MIRROR=http://mirrors.aliyun.com
RUN sed -i -e "s|http://deb.debian.org/debian|${DEBIAN_MIRROR}/debian|g" \
      -e "s|http://security.debian.org/debian-security|${DEBIAN_MIRROR}/debian-security|g" \
      /etc/apt/sources.list.d/debian.sources && \
    apt-get update && apt-get install -y --no-install-recommends \
      build-essential \
      ca-certificates \
      cmake \
      curl \
      ffmpeg \
      file \
      fonts-wqy-zenhei \
      git \
      jq \
      nodejs \
      npm \
      pkg-config \
      procps \
      ripgrep \
      unzip \
      xz-utils && \
    rm -rf /var/lib/apt/lists/*
