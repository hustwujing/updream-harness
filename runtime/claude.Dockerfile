ARG BASE_IMAGE=sandbox-harness-runtime-base:build
FROM ${BASE_IMAGE}
USER root
ARG CLAUDE_CODE_VERSION=2.1.76
RUN npm install -g @anthropic-ai/claude-code@${CLAUDE_CODE_VERSION} && npm cache clean --force
USER 10001:10001

