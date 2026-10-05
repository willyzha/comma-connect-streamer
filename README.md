<div align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="assets/icon-dark.png">
    <source media="(prefers-color-scheme: light)" srcset="assets/icon.png">
    <img alt="Comma Connect Streamer" src="assets/icon.png" width="120" height="120">
  </picture>
  <h1>Comma Connect Streamer</h1>
  <p>Download driving clips and track vehicle location from Comma.ai / openpilot devices, streaming video via RTSP (MediaMTX) and publishing live GPS tracking directly to Home Assistant via MQTT.</p>
</div>

## Features

- **RTSP Video Stream**: Continuously streams recent dashcam clips via MediaMTX and Linux FIFOs with seamless fallback to "Offline" and "Loading" screens.
- **HUD Overlays**: Burn-in timestamps and route metadata directly onto the video stream.
- **Home Assistant MQTT Device Tracker**: Native GPS tracker with auto-discovery, interactive map positioning, zone tracking (`home` / `not_home`), live speed, bearing, and location source diagnostics.
- **Multi-Tiered GPS Fallback**: Seamlessly resolves location using live Athena RPC, cached device GPS, or parked coordinates from the latest drive.
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
│   └── comma_mqtt.py       # Home Assistant MQTT location & sensor publisher
├── assets/                 # Project assets and video clip fallbacks
│   ├── icon.png            # Project icon for light mode (black comma with white play button)
│   ├── icon-dark.png       # Project icon for dark mode (white comma with dark play button)
│   ├── icon.svg            # Scalable vector icon source
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

## Home Assistant MQTT Integration

The streamer directly publishes to Home Assistant using **MQTT Auto-Discovery**. Home Assistant will automatically create a device card under **Settings > Devices & Services > MQTT** containing:
* **Device Tracker** (`device_tracker.comma_<dongle_id>`): Displays live vehicle position on the Home Assistant map and automatically calculates zone entry/exit (`home`, `work`, `not_home`).
* **Vehicle Speed Sensor** (`sensor.comma_<dongle_id>_speed`): Real-time speed with configurable units (`km/h`, `mph`, `m/s`).
* **Location Source Sensor** (`sensor.comma_<dongle_id>_source`): Diagnostic sensor showing GPS source (`athena_live`, `device_cached`, or `last_route_parked`).
* **Bearing Sensor** (`sensor.comma_<dongle_id>_bearing`): Compass heading in degrees.
* **Availability Tracking**: Uses MQTT Last Will and Testament (LWT) to mark entities as online/offline automatically.

### Enabling MQTT in `.env`:
```ini
ENABLE_MQTT=True
MQTT_HOST=192.168.1.100
MQTT_PORT=1883
MQTT_USER=homeassistant
MQTT_PASSWORD=your_mqtt_password
MQTT_DEVICE_NAME=Toyota Corolla
MQTT_SPEED_UNIT=km/h
LOCATION_POLL_INTERVAL=60
```

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `COMMA_DONGLE_ID` | Required | 16-character Comma Dongle ID |
| `COMMA_JWT_KEY` | Required | Comma JWT token |
| `WRITE_TIMESTAMPS` | `True` | Burn-in timestamps and route info on stream |
| `DISABLE_COMMA` | `False` | Disable driving clip downloader |
| `TIME_RANGE_DAYS` | `3` | How many days back to search for driving clips |
| `ENABLE_MQTT` | `False` | Enable Home Assistant MQTT device tracker & sensors |
| `MQTT_HOST` | `localhost` | MQTT broker hostname / IP |
| `MQTT_PORT` | `1883` | MQTT broker port |
| `MQTT_USER` | None | MQTT broker username (optional) |
| `MQTT_PASSWORD` | None | MQTT broker password (optional) |
| `MQTT_DEVICE_NAME` | `Comma <dongle_id>` | Custom friendly device name in Home Assistant |
| `MQTT_SPEED_UNIT` | `km/h` | Speed sensor unit (`km/h`, `mph`, `m/s`) |
| `LOCATION_POLL_INTERVAL` | `60` | Location polling interval in seconds |
| `LOADING_PATH` | `/app/assets/loading.ts` | Path to loading video screen |
| `OFFLINE_PATH` | `/app/assets/offline.ts` | Path to offline video screen |
