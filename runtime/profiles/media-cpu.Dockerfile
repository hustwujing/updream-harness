ARG COMMON_IMAGE=sandbox-harness-parallel-ali-common:build
FROM ${COMMON_IMAGE}
USER root
ARG PYPI_INDEX=http://mirrors.cloud.aliyuncs.com/pypi/simple/
RUN apt-get update && apt-get install -y --no-install-recommends dvisvgm ffmpeg imagemagick \
      libcairo2-dev libpango1.0-dev libpangocairo-1.0-0 libgl1-mesa-dri libglx-mesa0 \
      texlive-fonts-recommended texlive-latex-base texlive-latex-extra && \
    rm -rf /var/lib/apt/lists/*
COPY runtime/requirements-media.txt /tmp/requirements-profile.txt
RUN pip install --no-cache-dir --trusted-host mirrors.cloud.aliyuncs.com \
      --index-url ${PYPI_INDEX} -r /tmp/requirements-profile.txt && rm /tmp/requirements-profile.txt && \
    apt-get purge -y libcairo2-dev libpango1.0-dev && apt-get autoremove -y && \
    for command in sandbox-job sandbox-artifact sandbox-review sandbox-media sandbox-background sandbox-interact; do \
      ln -s /usr/local/bin/sandbox-runtime-cli "/usr/local/bin/$command"; done
USER 10001:10001
