ARG BASE_IMAGE=sandbox-harness-runtime-base:build
FROM ${BASE_IMAGE}
USER root
ARG PYPI_INDEX=http://mirrors.cloud.aliyuncs.com/pypi/simple/
ARG DEEPSEEK_HARNESS_VERSION=0.2.1rc2
RUN pip install --no-cache-dir --trusted-host mirrors.cloud.aliyuncs.com --index-url ${PYPI_INDEX} deepseek-harness-sdk==${DEEPSEEK_HARNESS_VERSION} && \
    python -c "import importlib.metadata as m; assert m.version('deepseek-harness-sdk') == '${DEEPSEEK_HARNESS_VERSION}'"
USER 10001:10001
