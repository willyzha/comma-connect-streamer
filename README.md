# Comma Connect Streamer

This project downloads clips from Comma.ai servers and streams them as an RTSP video stream via MediaMTX.

## Features
- Automatically downloads recent dashcam clips from Comma.ai.
- Streams video to an RTSP endpoint using MediaMTX and Linux FIFOs.
- Overlay timestamps and route information on the video.
- Continuous "Offline" and "Loading" screens when no clips are active.
- Fully Dockerized for easy deployment.

## Quick Start

1. **Clone the repository.**
2. **Create your configuration**:
   ```bash
   cp config.example.ini config.docker.ini
   ```
   Edit `config.docker.ini` and add your `JWT_KEY`.
3. **Launch with Docker Compose**:
   ```bash
   docker compose up --build -d
   ```
4. **Access the stream**:
   Open VLC or ffplay and connect to:
   `rtsp://your-ip:8554/comma_dashcam`

## Environment Variables
- `COMMA_JWT_KEY`: Override the JWT key in `config.ini`.
- `DISABLE_COMMA`: Set to `true` to stop the Comma download script.

## Configuration
Most settings can be adjusted in `config.docker.ini` before building, or mapped via volumes for live updates.
