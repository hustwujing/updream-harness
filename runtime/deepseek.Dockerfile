ARG BASE_IMAGE=sandbox-harness-runtime-base:build
FROM ${BASE_IMAGE}
USER root
ARG DEEPSEEK_HARNESS_WHEEL=/tmp/deepseek_harness.whl
COPY wheels/deepseek_harness.whl ${DEEPSEEK_HARNESS_WHEEL}
RUN pip install --no-cache-dir ${DEEPSEEK_HARNESS_WHEEL}
USER 10001:10001

