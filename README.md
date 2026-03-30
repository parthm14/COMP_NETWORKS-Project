# WTSP — Wind Turbine Space Protocol (Computer Networks Project)

This project simulates communication between:
1. A wind turbine node
2. A LEO satellite relay
3. A control station

It uses raw UDP with a custom WTSP binary protocol and Stop‑and‑Wait ARQ.

## Requirements
- Python 3.10+ (tested with 3.12)
- No extra deps for core system
- Streamlit optional for UI

## Quick Start (Local Demo)

Open three terminals in the repo:

**Terminal 1 (Satellite)**
```
./run_satellite.sh
```

**Terminal 2 (Turbine)**
```
./run_turbine.sh
```

**Terminal 3 (Station)**
```
./run_station.sh
```

You should see:
- `HELLO_ACK`, `NEGOTIATE_ACK`, and handshake complete
- Link UP and RTT on the station dashboard
- Telemetry updating

## Streamlit UI (Optional)

Install:
```
pip install streamlit
```

Run:
```
./run_streamlit.sh
```

Open:
```
http://localhost:8501
```

In the sidebar click **Connect listeners**.

## Full Core Test Checklist

In the station CLI:
```
yaw 45
pitch 10
fault
clear
```

Confirm:
- `CMD_ACK` for yaw/pitch
- Actuator values change over time
- Fault appears and clears correctly
- Alarms clear after `clear`

## Bonus Task 1 (Bi‑Directional O+M Video)

This runs automatically. You should see:
- Station logs: `[VIDEO] RX frame ... from turbine`
- Turbine logs: `[VIDEO] RX frame ... from station`
- Streamlit video panel shows `video_in` and `video_out`

## Bonus Task 3 (Malicious Detection / Mitigation)

Test an invalid command:
```
pitch 200
```

Expected:
- Station logs `NACK` with reason
- SECURITY alert appears in alarms
- Streamlit security panel shows security alert

## 3‑Machine Deployment

Edit `config.json`:
- `turbine.host` = Machine 1 IP
- `satellite.host` = Machine 2 IP
- `station.host` = Machine 3 IP

Run:
- Machine 1: `./run_turbine.sh`
- Machine 2: `./run_satellite.sh`
- Machine 3: `./run_station.sh`
- Optional UI on Machine 3: `./run_streamlit.sh`

## Files of Interest
- `common/protocol.py` — WTSP binary protocol
- `common/reliable_udp.py` — Stop‑and‑Wait ARQ
- `common/channel.py` — LEO channel simulator
- `turbine/turbine.py` — Turbine services + physics model
- `satellite/satellite.py` — Relay + channel effects
- `control_station/station.py` — Station dashboard + CLI
- `control_station/streamlit_app.py` — Streamlit UI
