# Comma Connect Streamer

Download driving clips and track vehicle location from Comma.ai / openpilot devices, streaming video via RTSP (MediaMTX) and publishing location data to Traccar and Home Assistant MQTT.

## Features

- **RTSP Video Stream**: Continuously streams recent dashcam clips via MediaMTX and Linux FIFOs with seamless fallback to "Offline" and "Loading" screens.
- **HUD Overlays**: Burn-in timestamps and route metadata directly onto the video stream.
- **Traccar Integration**: Publishes vehicle location to Traccar using OsmAnd HTTP protocol with multi-tiered fallback (Athena RPC, cached GPS, and parked route coordinates).
- **Home Assistant MQTT**: Publishes MQTT auto-discovery device tracker for Home Assistant.
- **Dockerized**: Easy single-container deployment with pre-built multi-service supervisor.

## Repository Structure

```
├── src/                    # Python application modules
│   ├── __init__.py
│   ├── comma_api.py        # Comma API client and multi-tiered location resolver
│   ├── comma_auth.py       # JWT authentication and caching manager
│   ├── automate_login.py   # Headless browser token renewal automation
│   ├── comma_download.py   # Video segment downloader and processing loop
│   ├── fifo_streamer.py    # FIFO queue and video transition streamer
│   ├── comma_mqtt.py       # Home Assistant MQTT location publisher
│   └── comma_traccar.py    # Traccar OsmAnd protocol location publisher
├── assets/                 # Video clip fallback assets
│   ├── loading.ts          # Transition clip played while loading new drives
│   └── offline.ts          # Looping clip played when no drives are active
├── docker/                 # Container runtime configurations
│   ├── docker-start.sh     # Container multi-process entrypoint script
│   └── mediamtx.yml        # MediaMTX RTSP/WebRTC/HLS/RTMP server configuration
├── .env.example            # Configuration template
├── Dockerfile              # Container image build definition
├── docker-compose.yml      # Docker Compose deployment definition
├── requirements.txt        # Python package dependencies
└── README.md
```

## Quick Start

### 1. Configure Environment
Copy the example environment configuration:
```bash
cp .env.example .env
```
Edit `.env` and set your device details:
* `COMMA_DONGLE_ID`: Your Comma 3/3X Dongle ID (found in Comma Connect).
* `COMMA_JWT_KEY`: Your Comma.ai JWT token (from [jwt.comma.ai](https://jwt.comma.ai)).

### 2. Launch with Docker Compose
```bash
docker compose up --build -d
```

### 3. Access Video Stream
Open VLC, ffplay, or any RTSP client and connect to:
* **RTSP**: `rtsp://<server-ip>:8554/comma_dashcam`
* **HLS**: `http://<server-ip>:8888/comma_dashcam`
* **WebRTC**: `http://<server-ip>:8889/comma_dashcam`

## Optional Integrations

### Traccar GPS Publishing
Enable Traccar integration in `.env`:
```ini
ENABLE_TRACCAR=True
TRACCAR_URL=http://your-traccar-server:5055
TRACCAR_DEVICE_ID=your_device_id
```

### Home Assistant MQTT
Enable MQTT integration in `.env`:
```ini
ENABLE_MQTT=True
MQTT_HOST=192.168.1.100
MQTT_PORT=1883
MQTT_USER=homeassistant
MQTT_PASSWORD=your_password
```

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `COMMA_DONGLE_ID` | Required | 16-character Comma Dongle ID |
| `COMMA_JWT_KEY` | Required | Comma JWT token |
| `WRITE_TIMESTAMPS` | `True` | Burn-in timestamps and route info on stream |
| `DISABLE_COMMA` | `False` | Disable driving clip downloader |
| `TIME_RANGE_DAYS` | `3` | How many days back to search for driving clips |
| `ENABLE_TRACCAR` | `False` | Enable Traccar location publisher |
| `TRACCAR_URL` | `http://localhost:5055` | Traccar server OsmAnd endpoint |
| `ENABLE_MQTT` | `False` | Enable Home Assistant MQTT publisher |
| `MQTT_HOST` | `localhost` | MQTT broker hostname / IP |
| `LOCATION_POLL_INTERVAL` | `60` | Location polling interval in seconds |
| `LOADING_PATH` | `/app/assets/loading.ts` | Path to loading video screen |
| `OFFLINE_PATH` | `/app/assets/offline.ts` | Path to offline video screen |
