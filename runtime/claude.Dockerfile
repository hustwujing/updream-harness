ARG CLAUDE_CODE_VERSION=2.1.76
ARG CLAUDE_SOURCE_IMAGE=127.0.0.1:25001/sandbox-harness/claude@sha256:064b95b0ff17c086137274675d9bd8204e6a234efd43b7c31b90b38ea9ef84ce
ARG BASE_IMAGE=sandbox-harness-runtime-base:build
FROM ${CLAUDE_SOURCE_IMAGE} AS harness_source
FROM ${BASE_IMAGE}
USER root
ARG CLAUDE_CODE_VERSION
COPY --from=harness_source /usr/local/lib/node_modules/@anthropic-ai/claude-code /usr/local/lib/node_modules/@anthropic-ai/claude-code
RUN ln -s /usr/local/lib/node_modules/@anthropic-ai/claude-code/cli.js /usr/local/bin/claude && \
    node -e "if(require('/usr/local/lib/node_modules/@anthropic-ai/claude-code/package.json').version !== '${CLAUDE_CODE_VERSION}') process.exit(1)"
USER 10001:10001
