FROM docker.m.daocloud.io/library/python:3.13-slim-bookworm@sha256:413041be39649714885528a06586416279db85744b6dcdf290af0ae557d782f6
ARG DEBIAN_MIRROR=http://mirrors.aliyun.com
ARG SANDBOX_USERNAME=updream
RUN sed -i -e "s|http://deb.debian.org/debian|${DEBIAN_MIRROR}/debian|g" \
      -e "s|http://security.debian.org/debian-security|${DEBIAN_MIRROR}/debian-security|g" \
      /etc/apt/sources.list.d/debian.sources && \
    apt-get update && apt-get install -y --no-install-recommends ca-certificates tini && \
    rm -rf /var/lib/apt/lists/* && \
    echo "${SANDBOX_USERNAME}" | grep -Eq '^[a-z_][a-z0-9_-]{1,31}$' && \
    groupadd --gid 10001 "${SANDBOX_USERNAME}" && \
    useradd --uid 10001 --gid 10001 --home-dir /workspace/state/home --no-create-home \
      --shell /bin/bash "${SANDBOX_USERNAME}" && \
    mkdir -p /opt/bootstrap /opt/platform /workspace/project /workspace/state/home \
      /workspace/inbox /workspace/outputs /workspace/tmp /run/harness && \
    chown -R 10001:10001 /workspace /run/harness && \
    chmod 1777 /workspace /workspace/project /workspace/state /workspace/state/home /workspace/inbox /workspace/outputs /workspace/tmp
COPY src/sandbox_bootstrap /opt/bootstrap/sandbox_bootstrap
COPY runtime/bundle_public_key.pem /opt/bootstrap/bundle_public_key.pem
COPY runtime/requirements-runtime.txt /opt/bootstrap/requirements-runtime.txt
COPY runtime/bootstrap_entrypoint.sh /usr/local/bin/sandbox-harness-runtime
COPY runtime/runtime_cli_dispatch.sh /usr/local/bin/sandbox-runtime-cli
RUN chmod 0755 /usr/local/bin/sandbox-harness-runtime /usr/local/bin/sandbox-runtime-cli && \
    chmod -R a+rX /opt/bootstrap
ENV USER=${SANDBOX_USERNAME} LOGNAME=${SANDBOX_USERNAME} PYTHONPATH=/opt/bootstrap
USER 10001:10001
EXPOSE 8080
HEALTHCHECK --interval=10s --timeout=3s --retries=6 CMD ["python","-c","import urllib.request;urllib.request.urlopen('http://127.0.0.1:8080/health',timeout=2)"]
ENTRYPOINT ["/usr/bin/tini","--","python","-m","sandbox_bootstrap.main"]
