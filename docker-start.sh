#!/bin/bash

# --- 1. Configuration Setup ---
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

# --- 2. MediaMTX Config Generation ---
# Generate mediamtx.yml if it doesn't exist (allowing for easy deployment without a volume)
if [ ! -f "/app/mediamtx.yml" ]; then
  echo "Generating default mediamtx.yml..."
  cat <<EOF > /app/mediamtx.yml
paths:
  comma_dashcam:
    runOnInit: ffmpeg -re -i /dev/shm/new_clip.fifo -c copy -f rtsp rtsp://localhost:8554/comma_dashcam
    runOnInitRestart: yes
rtspAddress: :8554
rtmpAddress: :1935
hlsAddress: :8888
webrtcAddress: :8889
EOF
fi

# --- 3. Database Initialization ---
# Ensure the database file exists so SQLite doesn't fail
if [ ! -f "/app/comma_downloads.db" ]; then
  echo "Initializing empty database..."
  touch /app/comma_downloads.db
fi

# --- 4. Environment Setup ---
# Create FIFOs if they don't exist
mkdir -p /dev/shm
[[ -p /dev/shm/new_clip.fifo ]] || mkfifo /dev/shm/new_clip.fifo

# Ensure data directories exist
mkdir -p /data/dashcam/clips

# --- 5. Start Processes ---
echo "Starting MediaMTX..."
/usr/local/bin/mediamtx /app/mediamtx.yml &
MEDIAMTX_PID=$!

sleep 2

if [ "$DISABLE_COMMA" != "true" ]; then
  echo "Starting Comma Download script..."
  python /app/comma_download.py &
  COMMA_PID=$!
fi

cleanup() {
    echo "Shutting down..."
    [ ! -z "$MEDIAMTX_PID" ] && kill $MEDIAMTX_PID
    [ ! -z "$COMMA_PID" ] && kill $COMMA_PID
    wait $MEDIAMTX_PID $COMMA_PID 2>/dev/null
    exit
}

trap cleanup SIGINT SIGTERM
wait -n
cleanup
