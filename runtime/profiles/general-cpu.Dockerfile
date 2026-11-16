ARG COMMON_IMAGE=sandbox-harness-parallel-common:build
FROM ${COMMON_IMAGE}
USER root
RUN for command in sandbox-job sandbox-artifact sandbox-review sandbox-background sandbox-interact; do \
      ln -s /usr/local/bin/sandbox-runtime-cli "/usr/local/bin/$command"; done
USER 10001:10001
