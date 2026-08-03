FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PLAYWRIGHT_BROWSERS_PATH=/ms-playwright
WORKDIR /app

RUN useradd --create-home --uid 1000 monitor
COPY pyproject.toml README.md /app/
COPY fb_monitor /app/fb_monitor
COPY scripts /app/scripts
RUN pip install --upgrade pip && pip install . \
    && python -m playwright install --with-deps chromium \
    && apt-get update \
    && apt-get install -y --no-install-recommends novnc websockify x11vnc \
    && rm -rf /var/lib/apt/lists/* \
    && chmod 755 /app/scripts/browser-login.sh

ARG APP_VERSION=development
ARG APP_UPDATED_AT=
ENV APP_VERSION=${APP_VERSION} APP_UPDATED_AT=${APP_UPDATED_AT}
RUN mkdir -p /data /browser-data && chown -R monitor:monitor /app /data /browser-data /ms-playwright

USER monitor
EXPOSE 8080 6080
CMD ["fb-monitor", "run"]
