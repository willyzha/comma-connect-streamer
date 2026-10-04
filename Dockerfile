# --- Downloader Stage ---
FROM alpine:latest AS downloader
ARG MEDIAMTX_VERSION=v1.9.3
RUN apk add --no-cache curl tar
RUN curl -L "https://github.com/bluenviron/mediamtx/releases/download/${MEDIAMTX_VERSION}/mediamtx_${MEDIAMTX_VERSION}_linux_amd64.tar.gz" \
    | tar -xz -C /tmp mediamtx mediamtx.yml
# --- Final Stage ---
FROM python:3.11-slim

# Install runtime dependencies
# ffmpeg: for video processing
# bash: for the startup script
# fontconfig: for managing fonts
# ttf-roboto: standard system package for Roboto fonts
# chromium: for playwright automation
RUN apt-get update && apt-get install -y \
    ffmpeg \
    bash \
    fontconfig \
    fonts-roboto \
    chromium \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Set environment variables for Playwright & Python
ENV PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=1
ENV PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH=/usr/bin/chromium
ENV PYTHONPATH=/app/src:/app

# Set working directory
WORKDIR /app

# Copy MediaMTX from downloader
COPY --from=downloader /tmp/mediamtx /usr/local/bin/mediamtx

# Copy project files
COPY . .

# Set environment variables for defaults
ENV FFMPEG_PATH=/usr/bin/ffmpeg
ENV DOWNLOAD_PATH=/data
ENV FIFO_PATH=/dev/shm/new_clip.fifo
ENV LOADING_PATH=/app/assets/loading.ts
ENV OFFLINE_PATH=/app/assets/offline.ts

# Create necessary directories
RUN mkdir -p /data/dashcam/clips /config

# Expose ports for MediaMTX
EXPOSE 8554 1935 8888 8889

# Entrypoint script
RUN chmod +x /app/docker/docker-start.sh

CMD ["/app/docker/docker-start.sh"]
