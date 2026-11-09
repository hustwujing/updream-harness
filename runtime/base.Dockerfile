FROM docker.m.daocloud.io/library/python:3.13-slim-bookworm@sha256:413041be39649714885528a06586416279db85744b6dcdf290af0ae557d782f6
ARG DEBIAN_MIRROR=http://mirrors.aliyun.com/debian
RUN sed -i "s|http://deb.debian.org|${DEBIAN_MIRROR}|g" /etc/apt/sources.list.d/debian.sources && \
    apt-get update && apt-get install -y --no-install-recommends ffmpeg nodejs npm ca-certificates git && \
    rm -rf /var/lib/apt/lists/*
WORKDIR /opt/platform
COPY . /opt/platform
RUN pip install --no-cache-dir . && mkdir -p /workspace/project /workspace/state /workspace/inbox /workspace/outputs /workspace/tmp /run/harness && chown -R 10001:10001 /workspace /run/harness
USER 10001:10001
EXPOSE 8080
ENTRYPOINT ["sandbox-harness-runtime"]
