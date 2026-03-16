#!/bin/bash

# --- 1. Directory Setup ---
# Create config and RAM data directories if they don't exist
mkdir -p /config
mkdir -p /dev/shm/dashcam/clips

# Create FIFOs if they don't exist (CRITICAL: must exist before MediaMTX starts)
[[ -p /dev/shm/new_clip.fifo ]] || mkfifo /dev/shm/new_clip.fifo

# --- 2. Configuration Setup ---
# We prioritize .env in the mounted /config directory.
if [ ! -f "/config/.env" ]; then
  echo "No .env file found in /config. Initializing with defaults from .env.example..."
  cp /app/.env.example /config/.env
fi

# Link /config/.env to where the app expects it
ln -sf /config/.env /app/.env

# --- 3. MediaMTX Config Generation ---
# We always generate this in /tmp so it's ephemeral and stays up-to-date with image updates
echo "Generating ephemeral mediamtx.yml in /tmp..."
cat <<EOF > /tmp/mediamtx.yml
paths:
  comma_dashcam:
    runOnInit: ffmpeg -loglevel error -re -i /dev/shm/new_clip.fifo -c:v libx264 -f mpegts udp://238.0.0.1:1234?pkt_size=1316
    runOnInitRestart: yes
    source: udp://238.0.0.1:1234
rtspAddress: :8554
rtmpAddress: :1935
hlsAddress: :8888
webrtcAddress: :8889
EOF

# --- 4. Database Initialization ---
if [ ! -f "/config/comma_downloads.db" ]; then
  echo "Initializing empty database in /config..."
  touch /config/comma_downloads.db
fi

# --- 5. Start Processes ---
echo "Starting MediaMTX..."
/usr/local/bin/mediamtx /tmp/mediamtx.yml &
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
