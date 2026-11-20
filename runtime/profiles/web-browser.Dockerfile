ARG COMMON_IMAGE=sandbox-harness-parallel-common:build
FROM ${COMMON_IMAGE}
USER root
ARG NPM_REGISTRY=https://registry.npmmirror.com
ARG PLAYWRIGHT_DOWNLOAD_HOST=https://npmmirror.com/mirrors/playwright
RUN apt-get update && apt-get install -y --no-install-recommends chromium libgbm1 \
      libgl1-mesa-dri libglx-mesa0 fonts-noto-color-emoji fonts-wqy-zenhei && \
    rm -rf /var/lib/apt/lists/* && \
    PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=1 npm install -g --registry="${NPM_REGISTRY}" \
      playwright@1.65.0 three@0.187.0 esbuild@0.28.2 react@19.3.1 \
      react-dom@19.3.1 typescript@7.0.2 pnpm@12.18.0 tailwindcss@3.4.1 \
      postcss@8.5.33 autoprefixer@10.7.0 tailwindcss-animate@1.0.7 \
      class-variance-authority@0.7.1 clsx@2.1.1 tailwind-merge@3.7.1 \
      lucide-react@1.76.0 next-themes@0.4.6 cmdk@1.1.1 embla-carousel-react@8.6.0 \
      react-day-picker@10.0.2 react-hook-form@7.92.0 react-resizable-panels@4.24.1 \
      sonner@2.0.8 vaul@1.1.2 \
      @radix-ui/react-accordion@1.3.1 @radix-ui/react-aspect-ratio@1.2.0 \
      @radix-ui/react-avatar@1.3.1 @radix-ui/react-checkbox@1.4.1 \
      @radix-ui/react-collapsible@1.2.1 @radix-ui/react-context-menu@2.4.1 \
      @radix-ui/react-dialog@1.3.1 @radix-ui/react-dropdown-menu@2.2.1 \
      @radix-ui/react-hover-card@1.2.1 @radix-ui/react-label@2.1.17 \
      @radix-ui/react-menubar@1.2.1 @radix-ui/react-navigation-menu@1.4.1 \
      @radix-ui/react-popover@1.3.1 @radix-ui/react-progress@1.2.1 \
      @radix-ui/react-radio-group@1.5.1 @radix-ui/react-scroll-area@1.4.0 \
      @radix-ui/react-select@2.4.1 @radix-ui/react-separator@1.2.0 \
      @radix-ui/react-slider@1.6.1 @radix-ui/react-slot@1.5.0 \
      @radix-ui/react-switch@1.4.1 @radix-ui/react-tabs@1.2.1 \
      @radix-ui/react-toast@1.3.1 @radix-ui/react-toggle@1.2.1 \
      @radix-ui/react-toggle-group@1.2.1 @radix-ui/react-tooltip@1.4.1 && \
    PLAYWRIGHT_DOWNLOAD_HOST="${PLAYWRIGHT_DOWNLOAD_HOST}" PLAYWRIGHT_BROWSERS_PATH=/ms-playwright \
      playwright install chromium && chmod -R a+rX /ms-playwright && npm cache clean --force
COPY runtime/requirements-web.txt /tmp/requirements-profile.txt
RUN pip install --no-cache-dir --trusted-host mirrors.cloud.aliyuncs.com \
      --index-url http://mirrors.cloud.aliyuncs.com/pypi/simple/ -r /tmp/requirements-profile.txt && \
    rm /tmp/requirements-profile.txt && \
    for command in sandbox-job sandbox-artifact sandbox-review sandbox-background sandbox-interact; do \
      ln -s /usr/local/bin/sandbox-runtime-cli "/usr/local/bin/$command"; done
COPY runtime/browser_screenshot.mjs /usr/local/bin/sandbox-browser-screenshot
COPY runtime/browser_render.mjs /usr/local/bin/sandbox-browser-render
COPY runtime/chromium_proxy.cjs /usr/local/bin/chromium
RUN chmod 0755 /usr/local/bin/sandbox-browser-screenshot /usr/local/bin/sandbox-browser-render \
      /usr/local/bin/chromium
ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright CHROME_PATH=/usr/bin/chromium
USER 10001:10001
