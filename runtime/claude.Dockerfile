ARG BASE_IMAGE=sandbox-harness-runtime-base:build
FROM ${BASE_IMAGE}
USER root
ARG CLAUDE_CODE_VERSION=2.1.76
ARG NPM_REGISTRY=https://registry.npmmirror.com
RUN npm install -g --registry=${NPM_REGISTRY} @anthropic-ai/claude-code@${CLAUDE_CODE_VERSION} && npm cache clean --force
USER 10001:10001
