ARG CODEX_VERSION=0.153.4
ARG CODEX_SOURCE_IMAGE=127.0.0.1:25001/sandbox-harness/codex@sha256:ca0d1754d7d4449af2e02b374c3968ea0d5875cf3a69dd5327e14a7029b9b5d4
ARG PROFILE_IMAGE=sandbox-harness-parallel-ali-profile:build
FROM ${CODEX_SOURCE_IMAGE} AS harness_source
FROM ${PROFILE_IMAGE}
USER root
ARG CODEX_VERSION
COPY --from=harness_source /usr/local/lib/node_modules/@openai/codex /usr/local/lib/node_modules/@openai/codex
RUN ln -s /usr/local/lib/node_modules/@openai/codex/bin/codex.js /usr/local/bin/codex && \
    node -e "if(require('/usr/local/lib/node_modules/@openai/codex/package.json').version !== '${CODEX_VERSION}') process.exit(1)"
USER 10001:10001
