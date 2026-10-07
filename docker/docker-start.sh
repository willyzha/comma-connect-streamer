#!/bin/bash

# --- 1. Directory Setup ---
# Create config and RAM data directories if they don't exist
mkdir -p /config
mkdir -p /dev/shm/dashcam/clips

# Create FIFOs if they don't exist (CRITICAL: must exist before MediaMTX starts)
[[ -p /dev/shm/new_clip.fifo ]] || mkfifo /dev/shm/new_clip.fifo

# --- 2. Configuration Setup ---
# Check for visible config.conf first, then config.env, legacy .env, or initialize from config.conf.example
if [ -f "/config/config.conf" ]; then
  echo "Found configuration in /config/config.conf"
elif [ -f "/config/config.env" ]; then
  echo "Found configuration in /config/config.env. Migrating to /config/config.conf..."
  cp /config/config.env /config/config.conf
elif [ -f "/config/.env" ]; then
  echo "Found legacy /config/.env. Migrating to visible /config/config.conf..."
  cp /config/.env /config/config.conf
else
  echo "No configuration file found in /config. Initializing visible config from config.conf.example..."
  if [ -f "/app/config.conf.example" ]; then
    cp /app/config.conf.example /config/config.conf
  elif [ -f "/app/config.env.example" ]; then
    cp /app/config.env.example /config/config.conf
  elif [ -f "/app/.env.example" ]; then
    cp /app/.env.example /config/config.conf
  fi
fi

# Link configuration to app working directory
ln -sf /config/config.conf /app/config.conf
ln -sf /config/config.conf /app/config.env
ln -sf /config/config.conf /app/.env

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

DISABLE_COMMA=$(python3 -c "from comma_api import get_config; print(str(get_config('DISABLE_COMMA', False, bool)).lower())")
if [ "$DISABLE_COMMA" != "true" ]; then
  echo "Starting Comma Download script..."
  python /app/src/comma_download.py &
  COMMA_PID=$!
fi

ENABLE_MQTT=$(python3 -c "from comma_api import get_config; print(str(get_config('ENABLE_MQTT', False, bool)).lower())")
if [ "$ENABLE_MQTT" = "true" ]; then
  echo "Starting Comma MQTT script..."
  python /app/src/comma_mqtt.py &
  MQTT_PID=$!
fi

cleanup() {
    echo "Shutting down..."
    [ ! -z "$MEDIAMTX_PID" ] && kill $MEDIAMTX_PID 2>/dev/null
    [ ! -z "$COMMA_PID" ] && kill $COMMA_PID 2>/dev/null
    [ ! -z "$MQTT_PID" ] && kill $MQTT_PID 2>/dev/null
    wait $MEDIAMTX_PID $COMMA_PID $MQTT_PID 2>/dev/null
    exit
}

trap cleanup SIGINT SIGTERM
# Wait for the primary streaming server process (MediaMTX)
wait $MEDIAMTX_PID
cleanup
