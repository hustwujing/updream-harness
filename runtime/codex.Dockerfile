ARG BASE_IMAGE=sandbox-harness-runtime-base:build
FROM ${BASE_IMAGE}
USER root
ARG CODEX_VERSION=0.153.4
ARG NPM_REGISTRY=https://registry.npmmirror.com
RUN npm install -g --registry=${NPM_REGISTRY} @openai/codex@${CODEX_VERSION} && npm cache clean --force
USER 10001:10001
