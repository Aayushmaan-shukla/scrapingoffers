FROM python:3.12-slim

ENV DEBIAN_FRONTEND=noninteractive \
    PIP_NO_CACHE_DIR=1 \
    PYTHONUNBUFFERED=1 \
    UDC_DISABLE_VERSION_CHECK=1

# Install Chrome (stable) + deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget gnupg ca-certificates unzip fonts-liberation libnss3 libasound2 libxss1 libxshmfence1 libgbm1 libu2f-udev \
    && wget -qO - https://dl.google.com/linux/linux_signing_key.pub | gpg --dearmor -o /usr/share/keyrings/google-linux.gpg \
    && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-linux.gpg] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list \
    && apt-get update && apt-get install -y --no-install-recommends google-chrome-stable \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY enhanced_flipkart_scraper_comprehensive.py .

# Default env (override as needed)
ENV INPUT_FILE=/app/data/all_data.json \
    OUTPUT_FILE=/app/data/all_data_out.json \
    FAST=1 \
    SESSION_BATCH_SIZE=150

# Entry: use shard env if provided
ENTRYPOINT ["python","-u","enhanced_flipkart_scraper_comprehensive.py"]
CMD ["--input-file","/app/data/all_data.json","--fast"]