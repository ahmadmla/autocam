# AutoCam

AutoCam is a real-time indoor tracking system for automated camera framing. It uses multiple Raspberry Pi-connected UWB nodes to collect range data, schedules those nodes over MQTT, solves a bounded 2D pose from anchor distances, filters the result for stability, and writes live history for a browser dashboard.

This repo contains the end-to-end tracking stack:

- distributed node control over MQTT
- central pose solving and filtering in Python
- live trail/history visualization in the browser
- DWM3001 firmware work in C for faster node switching

Homography-based camera mapping is still in progress.

## What It Does

Each node listens to a DWM3001-based UWB device over serial, captures `SESSION_INFO_NTF` measurement blocks, and publishes normalized anchor readings to MQTT. A central logger consumes those readings, converts slant ranges into planar ranges, runs bounded 2D multilateration, rejects obvious outliers, applies motion-aware filtering, and saves the resulting pose stream to disk for visualization.

The dashboard reads `central_history.json` and `anchors.json` to render:

- anchor layout
- current tag position
- per-node motion trails
- live playback / history scrubbing

## System Architecture

1. `node_sender.py`
   Reads serial output from one UWB node, responds to `activate` / `pause` commands, and publishes anchor snapshots to MQTT.
2. `scheduler.py`
   Runs round-robin slot scheduling so only one node is actively ranging at a time, with timeout handling and cooldown logic.
3. `pose_logger_runtime.py`
   Consumes MQTT payloads, maintains per-node tracking pipelines, filters ranges, solves pose, scores solution quality, and writes history/log output.
4. `dashboard.html`
   Displays the room geometry, anchor footprint, current pose, and motion trail from the generated history file.
5. `run_central_stack.py`
   Launches the central logger, scheduler, and local static server together from one terminal.

## Pose Pipeline

The pose stack is split into `uwb_pose/` and does more than a raw trilateration pass.

- `io_helpers.py` parses serial measurement blocks and atomically writes history JSON.
- `geometry.py` loads anchor layouts from `anchors.json`, builds room bounds, and constrains poses to the valid footprint.
- `solver.py` handles range sanitization, planar conversion, per-anchor filtering, geometry-aware weighting, damped Gauss-Newton multilateration, and one-pass outlier retry.
- `pose_logger_runtime.py` applies motion-aware tracking, deadbanding, stationary analysis, confidence scoring, and reporting logic.

Key behaviors implemented here:

- median + exponential filtering on anchor ranges
- stale-measurement handling with reduced weight
- bounded 2D multilateration inside the anchor footprint
- worst-anchor outlier rejection when retry materially improves RMSE
- quality scoring based on anchors used, freshness, RMSE, and max residual
- motion/stationary-aware pose smoothing to reduce jitter

## Repo Layout

- `node_sender.py` - node-side serial reader + MQTT publisher
- `scheduler.py` - central round-robin node scheduler
- `pose_logger.py` - thin entrypoint for the central logger
- `pose_logger_runtime.py` - central runtime and tracking pipeline
- `run_central_stack.py` - one-command launcher for the central stack
- `anchors.json` - editable anchor layouts / room profiles
- `dashboard.html` - browser dashboard for live and recorded history
- `uwb_pose/config.py` - runtime configuration and tuning constants
- `uwb_pose/geometry.py` - anchor geometry and bounds handling
- `uwb_pose/solver.py` - multilateration, filtering, and quality logic
- `uwb_pose/io_helpers.py` - parsing, logging, and history writes
- `firmware/` - DWM3001 firmware workspace and SDK artifacts

## Requirements

- Python 3.10+
- an MQTT broker reachable by the nodes and central machine
- the Python packages listed in `requirements.txt`
- a browser for `dashboard.html`

Set up the Python environment with a virtual environment, but do not commit `.venv/` to git:

```bash
python -m venv .venv
.venv\Scripts\activate
python -m pip install -r requirements.txt
```

For environment setup, start from `.env.example` and replace the sample values with your own broker, node, serial-port, and anchor settings. Anything marked `# CHANGE` is expected to be reviewed before running the stack.

## Quick Start

### 1. Start an MQTT broker

Use Mosquitto or any broker that the Pis and central machine can reach.

### 2. Configure anchor layout

Edit `anchors.json` if you need to change room geometry or switch anchor profiles.

### 3. Set machine-specific environment variables

At minimum, update these values for your setup before you run anything:

- `MQTT_HOST` so every machine points at your broker
- `UWB_PORT` on each Raspberry Pi so it matches the attached serial device
- `NODE_ID` on each Raspberry Pi so each node is unique
- `UWB_NODES` on the central scheduler so it matches your deployed node IDs
- `UWB_FIRA_BLOCK_MS` if your firmware timing or ranging cadence needs a different block duration
- the active anchor profile in `anchors.json` so the coordinates match your room

### 4. Run the central stack

```bash
python run_central_stack.py
```

This starts:

- the central pose logger
- the scheduler
- a local static file server for the dashboard

By default the dashboard is served at:

```text
http://localhost:8000/autocam/dashboard.html
```

### 5. Run a node process on each Raspberry Pi

```bash
python node_sender.py
```

Each node needs its own environment configuration, especially:

- `NODE_ID`
- `UWB_PORT`
- `MQTT_HOST`
- `MQTT_PORT`

## Useful Environment Variables

### Node-side

- `NODE_ID`
- `UWB_PORT`
- `UWB_BAUD`
- `MQTT_HOST`
- `MQTT_PORT`
- `MQTT_RAW_TOPIC_BASE`
- `MQTT_CMD_TOPIC_BASE`
- `UWB_PEER_ADDRS`
- `UWB_FIRA_SLOT_RSTU`
- `UWB_FIRA_ROUND_SLOTS`
- `UWB_FIRA_BLOCK_MS`

### Central-side

- `MQTT_HOST`
- `MQTT_PORT`
- `MQTT_TOPIC`
- `CENTRAL_HISTORY_PATH`
- `ANCHORS_PATH`
- `VIS_HTTP_PORT`

Most tuning constants for filtering and solver behavior live in `uwb_pose/config.py`.

`UWB_FIRA_BLOCK_MS` deserves special attention on the node side: it controls the block duration passed into the generated FiRa init command. If another setup uses different firmware timing, a different number of peers, or a different tradeoff between responsiveness and measurement stability, this is one of the first values they may need to tune.

## Outputs

The main generated artifacts are:

- `central_history.json` - live pose history for the dashboard
- `central_pose_logger_output.txt` - event log from the central pipeline
- `central_raw_input_log.txt` - raw serial/ingest diagnostics

The node processes also emit their own raw serial and event logs based on `NODE_ID`.

## Firmware Note

The `firmware/` directory contains the DWM3001 firmware workspace used for timing-related changes that improved node switching responsiveness. The firmware work in this project was done in C.

If another user needs to put the matching firmware on the boards, they should plan on flashing the DWM3001 targets with SEGGER J-Flash Lite or an equivalent J-Link tool. In practice that means:

- connect the board over J-Link
- open J-Flash Lite and select the correct target device
- load the appropriate `.hex` file from `firmware/` for the board you are flashing
- flash the image, then power-cycle the device
- confirm the board comes back on the expected serial port and responds to the init / activate / pause command flow used by `node_sender.py`

## Current Status

Implemented:

- multi-node UWB scheduling over MQTT
- per-node serial measurement capture
- centralized bounded 2D pose estimation
- range filtering, outlier rejection, and confidence scoring
- live browser visualization of pose history

In progress:

- homography calibration from tracked room coordinates into camera/image space

## Context

AutoCam was developed as a practical tracking system for automated filming workflows, with ongoing feedback from music-performance use cases.
