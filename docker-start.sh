#!/bin/bash

# --- 1. Directory Setup ---
# Create config and RAM data directories if they don't exist
mkdir -p /config
mkdir -p /dev/shm/dashcam/clips

# --- 2. Configuration Setup ---
# We prioritize config.ini in the mounted /config directory.
# If it's missing, we try config.docker.ini (from build) or config.example.ini
if [ ! -f "/config/config.ini" ]; then
  echo "No config.ini found in /config. Initializing..."
  if [ -f "/app/config.docker.ini" ]; then
    cp /app/config.docker.ini /config/config.ini
  else
    cp /app/config.example.ini /config/config.ini
  fi
fi

# Link /config/config.ini to where the app expects it if it's not already there
# (The app looks in its local directory by default)
cp /config/config.ini /app/config.ini

# Support for environment variables to override the working config
if [ ! -z "$COMMA_JWT_KEY" ]; then
  sed -i "s|^JWT_KEY = .*|JWT_KEY = $COMMA_JWT_KEY|" /app/config.ini
  # Also sync back to /config for persistence
  sed -i "s|^JWT_KEY = .*|JWT_KEY = $COMMA_JWT_KEY|" /config/config.ini
fi

# --- 3. MediaMTX Config Generation ---
# Generate mediamtx.yml in /config if it doesn't exist
if [ ! -f "/config/mediamtx.yml" ]; then
  echo "Generating default mediamtx.yml in /config..."
  cat <<EOF > /config/mediamtx.yml
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

# --- 4. Database Initialization ---
if [ ! -f "/config/comma_downloads.db" ]; then
  echo "Initializing empty database in /config..."
  touch /config/comma_downloads.db
fi

# --- 5. Start Processes ---
echo "Starting MediaMTX..."
/usr/local/bin/mediamtx /config/mediamtx.yml &
MEDIAMTX_PID=$!

sleep 2

if [ "$DISABLE_COMMA" != "true" ]; then
  echo "Starting Comma Download script..."
  # Ensure the app uses the database path in /config as defined in config.ini
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
