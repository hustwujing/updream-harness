ARG COMMON_IMAGE=sandbox-harness-parallel-ali-common:build
FROM ${COMMON_IMAGE}
USER root
ARG PYPI_INDEX=http://mirrors.cloud.aliyuncs.com/pypi/simple/
COPY runtime/requirements-data.txt /tmp/requirements-profile.txt
RUN pip install --no-cache-dir --trusted-host mirrors.cloud.aliyuncs.com \
      --index-url ${PYPI_INDEX} -r /tmp/requirements-profile.txt && rm /tmp/requirements-profile.txt && \
    for command in sandbox-job sandbox-artifact sandbox-review sandbox-background sandbox-interact; do \
      ln -s /usr/local/bin/sandbox-runtime-cli "/usr/local/bin/$command"; done
USER 10001:10001
