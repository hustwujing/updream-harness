ARG COMMON_IMAGE=sandbox-harness-parallel-common:build
FROM ${COMMON_IMAGE}
USER root
ARG PYPI_INDEX=http://mirrors.cloud.aliyuncs.com/pypi/simple/
ARG NPM_REGISTRY=https://registry.npmmirror.com
RUN apt-get update && apt-get install -y --no-install-recommends libreoffice-calc libreoffice-core \
      libreoffice-impress libreoffice-writer pandoc poppler-utils qpdf tesseract-ocr \
      tesseract-ocr-chi-sim && rm -rf /var/lib/apt/lists/*
COPY runtime/requirements-office.txt /tmp/requirements-profile.txt
RUN pip install --no-cache-dir --trusted-host mirrors.cloud.aliyuncs.com \
      --index-url ${PYPI_INDEX} -r /tmp/requirements-profile.txt && rm /tmp/requirements-profile.txt && \
    npm install -g --registry="${NPM_REGISTRY}" docx@9.15.3 pptxgenjs@4.0.1 \
      react@19.3.1 react-dom@19.3.1 react-icons@5.7.0 sharp@0.35.5 && npm cache clean --force && \
    for command in sandbox-job sandbox-artifact sandbox-review sandbox-site sandbox-background sandbox-interact; do \
      ln -s /usr/local/bin/sandbox-runtime-cli "/usr/local/bin/$command"; done
USER 10001:10001
