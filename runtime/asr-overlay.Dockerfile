ARG ASR_SOURCE_IMAGE=sandbox-harness-runtime-base:0027ec835f16
ARG PROFILE_IMAGE=sandbox-harness-parallel-primary-full-cpu:build
ARG PROFILE_ID=primary-full-cpu
FROM ${ASR_SOURCE_IMAGE} AS asr_source
FROM ${PROFILE_IMAGE}
USER root
ARG PYPI_INDEX=http://mirrors.cloud.aliyuncs.com/pypi/simple/
COPY runtime/requirements-asr.txt /tmp/requirements-asr.txt
RUN pip install --no-cache-dir --trusted-host mirrors.cloud.aliyuncs.com \
      --index-url ${PYPI_INDEX} -r /tmp/requirements-asr.txt && rm /tmp/requirements-asr.txt
COPY --from=asr_source /opt/models/faster-whisper-small/ /opt/models/faster-whisper-small/
COPY --from=asr_source /opt/platform/runtime/asr-smoke.wav /opt/platform/runtime/asr-smoke.wav
COPY runtime/asr_cli.py runtime/asr_probe.py /opt/platform/runtime/
RUN printf '%s  %s\n' \
      b55496ac7940a7ae47d2c01eab40edfd8701feec1229d9cce3b40014383fb828 /opt/models/faster-whisper-small/config.json \
      3e305921506d8872816023e4c273e75d2419fb89b24da97b4fe7bce14170d671 /opt/models/faster-whisper-small/model.bin \
      fb7b63191e9bb045082c79fd742a3106a12c99513ab30df4a0d47fa6cb6fd0ab /opt/models/faster-whisper-small/tokenizer.json \
      34ce3fe1c5041027b3f8d42912270993f986dbc4bb34cf27f951e34a1e453913 /opt/models/faster-whisper-small/vocabulary.txt \
      | sha256sum -c - && \
    install -m 0755 /opt/platform/runtime/asr_cli.py /usr/local/bin/sandbox-transcribe && \
    chmod -R a+rX /opt/models && \
    python /opt/platform/runtime/asr_probe.py --model /opt/models/faster-whisper-small \
      --audio /opt/platform/runtime/asr-smoke.wav --expect "blue square" && \
    if [ "${PROFILE_ID}" = "primary-full-cpu" ]; then sandbox-original-compatibility-probe; fi
ENV SANDBOX_WHISPER_MODEL=/opt/models/faster-whisper-small
USER 10001:10001
