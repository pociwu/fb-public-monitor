FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /app

RUN useradd --create-home --uid 1000 monitor
COPY pyproject.toml README.md /app/
COPY fb_monitor /app/fb_monitor
RUN pip install --upgrade pip && pip install .
RUN mkdir -p /data && chown -R monitor:monitor /app /data

USER monitor
EXPOSE 8080
CMD ["fb-monitor", "run"]

