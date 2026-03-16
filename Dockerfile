# --- Downloader Stage ---
FROM alpine:latest AS downloader
ARG MEDIAMTX_VERSION=v1.9.3
RUN apk add --no-cache curl tar
RUN curl -L "https://github.com/bluenviron/mediamtx/releases/download/${MEDIAMTX_VERSION}/mediamtx_${MEDIAMTX_VERSION}_linux_amd64.tar.gz" \
    | tar -xz -C /tmp mediamtx mediamtx.yml

# --- Final Stage ---
FROM python:3.11-alpine

# Install minimal runtime dependencies
# ffmpeg: for video processing
# bash: for the startup script
RUN apk add --no-cache ffmpeg bash

# Install Python dependencies
RUN pip install --no-cache-dir requests watchdog

# Set working directory
WORKDIR /app

# Copy MediaMTX from downloader
COPY --from=downloader /tmp/mediamtx /usr/local/bin/mediamtx

# Copy project files (this includes your custom mediamtx.yml, config files, and scripts)
COPY . .

# Set environment variables for config.ini defaults
ENV FFMPEG_PATH=/usr/bin/ffmpeg
ENV DOWNLOAD_PATH=/data
ENV DATABASE_PATH=/app/comma_downloads.db
ENV FIFO_PATH=/dev/shm/new_clip.fifo

# Create data directory
RUN mkdir -p /data/dashcam/clips

# Expose ports for MediaMTX
EXPOSE 8554 1935 8888 8889

# Entrypoint script
RUN chmod +x /app/docker-start.sh

CMD ["/app/docker-start.sh"]
