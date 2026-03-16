#!/bin/bash

# Use config.docker.ini if it exists (local build), otherwise fallback to example
if [ -f "/app/config.docker.ini" ]; then
  cp /app/config.docker.ini /app/config.ini
else
  cp /app/config.example.ini /app/config.ini
fi

# Support for environment variables to override config.ini
if [ ! -z "$COMMA_JWT_KEY" ]; then
  sed -i "s|^JWT_KEY = .*|JWT_KEY = $COMMA_JWT_KEY|" /app/config.ini
fi
if [ ! -z "$COMMA_DATABASE_PATH" ]; then
  sed -i "s|^DATABASE_PATH = .*|DATABASE_PATH = $COMMA_DATABASE_PATH|" /app/config.ini
fi
if [ ! -z "$DOWNLOAD_PATH" ]; then
  sed -i "s|^DOWNLOAD_PATH = .*|DOWNLOAD_PATH = $DOWNLOAD_PATH|" /app/config.ini
fi

# Create FIFOs if they don't exist
# This helps MediaMTX's ffmpeg readers start correctly
mkdir -p /dev/shm
[[ -p /dev/shm/new_clip.fifo ]] || mkfifo /dev/shm/new_clip.fifo

# Ensure data directories exist
mkdir -p /data/dashcam/clips

# Start MediaMTX in the background
echo "Starting MediaMTX..."
/usr/local/bin/mediamtx /app/mediamtx.yml &
MEDIAMTX_PID=$!

# Wait for MediaMTX to start
sleep 2

# Start Comma Download script
if [ "$DISABLE_COMMA" != "true" ]; then
  echo "Starting Comma Download script..."
  python /app/comma_download.py &
  COMMA_PID=$!
fi

# Function to handle shutdown
cleanup() {
    echo "Shutting down..."
    [ ! -z "$MEDIAMTX_PID" ] && kill $MEDIAMTX_PID
    [ ! -z "$COMMA_PID" ] && kill $COMMA_PID
    wait $MEDIAMTX_PID $COMMA_PID 2>/dev/null
    exit
}

trap cleanup SIGINT SIGTERM

# Wait for any process to exit
wait -n

# If any process exits, kill the others and exit
cleanup
