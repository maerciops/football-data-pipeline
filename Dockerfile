FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    chromium \
    chromium-driver \
    xvfb \
    xauth \
    --no-install-recommends \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

ENV PYTHONUNBUFFERED=1
ENV OUTPUT_DIR=/app/data
ENV APP_BASE_DIR=/app

RUN mkdir -p /app/data && chmod 777 /app/data

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["bash", "-c", "Xvfb :99 -screen 0 1920x1080x24 & sleep 1 && export DISPLAY=:99 && python -u src/collectors/fbref.py"]