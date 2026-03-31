import json
import socket
import threading
import time
from collections import deque

import os
import plotly.graph_objects as go
import streamlit as st

_state_lock = threading.Lock()
_sat_lock = threading.Lock()
_state_data = {}
_sat_data = {}
_listener_error = None
_state_rx_count = 0
_sat_rx_count = 0
_state_last_rx = None
_sat_last_rx = None


def _init_history():
    if "hist" not in st.session_state:
        st.session_state["hist"] = {
            "time": deque(maxlen=120),
            "wind": deque(maxlen=120),
            "power": deque(maxlen=120),
            "rpm": deque(maxlen=120),
            "pitch": deque(maxlen=120),
            "yaw": deque(maxlen=120),
            "temp": deque(maxlen=120),
            "vib": deque(maxlen=120),
            "rtt": deque(maxlen=120),
        }


def _push_history(state):
    hist = st.session_state["hist"]
    sensor = state.get("sensor", {}) if state else {}
    telem = state.get("telemetry", {}) if state else {}
    hist["time"].append(time.strftime("%H:%M:%S"))
    hist["wind"].append(sensor.get("wind_speed_ms", 0))
    hist["power"].append(sensor.get("power_kw", 0))
    hist["rpm"].append(sensor.get("rotor_rpm", 0))
    hist["pitch"].append(sensor.get("pitch_deg", 0))
    hist["yaw"].append(sensor.get("yaw_deg", 0))
    hist["temp"].append(sensor.get("nacelle_temp_c", 0))
    hist["vib"].append(sensor.get("vibration_g", 0))
    hist["rtt"].append(state.get("rtt_ms", 0))


def _rerun():
    try:
        st.rerun()
    except AttributeError:
        st.experimental_rerun()


def _start_udp_listeners(state_port: int, sat_port: int, max_size: int = 65535):
    def _loop_state():
        global _listener_error, _state_rx_count, _state_last_rx
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind(("0.0.0.0", state_port))
        except OSError as exc:
            with _state_lock:
                _listener_error = f"State listener bind failed on {state_port}: {exc}"
            return
        while True:
            data, _ = sock.recvfrom(max_size)
            try:
                payload = json.loads(data.decode("utf-8"))
            except Exception:
                continue
            with _state_lock:
                _state_data.clear()
                _state_data.update(payload)
                _state_rx_count += 1
                _state_last_rx = time.time()

    def _loop_sat():
        global _listener_error, _sat_rx_count, _sat_last_rx
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind(("0.0.0.0", sat_port))
        except OSError as exc:
            with _sat_lock:
                _listener_error = f"Satellite listener bind failed on {sat_port}: {exc}"
            return
        while True:
            data, _ = sock.recvfrom(max_size)
            try:
                payload = json.loads(data.decode("utf-8"))
            except Exception:
                continue
            with _sat_lock:
                _sat_data.clear()
                _sat_data.update(payload)
                _sat_rx_count += 1
                _sat_last_rx = time.time()

    threading.Thread(target=_loop_state, daemon=True).start()
    threading.Thread(target=_loop_sat, daemon=True).start()


def _send_ui_cmd(host: str, port: int, cmd: str, value=None):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    payload = {"cmd": cmd}
    if value is not None:
        payload["value"] = value
    sock.sendto(json.dumps(payload).encode("utf-8"), (host, port))

def _send_test_packet(host: str, port: int, payload: dict):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.sendto(json.dumps(payload).encode("utf-8"), (host, port))

def _read_config_ui():
    try:
        base = os.path.dirname(os.path.abspath(__file__))
        cfg_path = os.path.join(base, "..", "config.json")
        with open(cfg_path, "r") as f:
            cfg = json.load(f)
        return cfg.get("ui", {}), cfg_path
    except Exception:
        return None, None


_CHART_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="Space Mono, monospace", color="#64748b", size=10),
    xaxis=dict(showgrid=True, gridcolor="#1e2d40", gridwidth=1,
               zeroline=False, color="#64748b"),
    yaxis=dict(showgrid=True, gridcolor="#1e2d40", gridwidth=1,
               zeroline=False, color="#64748b"),
    margin=dict(l=40, r=10, t=20, b=30),
    legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(size=9)),
    hovermode="x unified",
)


def _line_chart(x, y_series: dict, height=160, colors=None):
    fig = go.Figure()
    palette = ["#00c4b4", "#0066ff", "#ff6b35", "#ffd600", "#00e676"]
    for i, (name, y) in enumerate(y_series.items()):
        c = (colors or palette)[i % len(palette)]
        fig.add_trace(go.Scatter(
            x=list(x), y=list(y), name=name,
            mode="lines",
            line=dict(color=c, width=1.5),
            fill="tozeroy",
            fillcolor=f"rgba({int(c[1:3],16)},{int(c[3:5],16)},{int(c[5:7],16)},0.08)",
        ))
    fig.update_layout(**_CHART_LAYOUT, height=height)
    return fig


def main():
    st.set_page_config(page_title="WTSP Control Station", layout="wide")
    if "listeners_started" not in st.session_state:
        st.session_state["listeners_started"] = False
    if "listener_error" not in st.session_state:
        st.session_state["listener_error"] = ""
    if "auto_start_listeners" not in st.session_state:
        st.session_state["auto_start_listeners"] = True
    _init_history()
    st.sidebar.checkbox("Auto-refresh", value=True, key="auto_refresh")

    with st.sidebar:
        st.markdown("""
        <div style='text-align:center;padding:16px 0 8px'>
          <div style='font-family:Space Mono;font-size:0.65rem;
                      letter-spacing:0.2em;color:#64748b;'>WTSP</div>
          <div style='font-family:Space Mono;font-size:1.1rem;
                      color:#00c4b4;font-weight:700;'>CONTROL STATION</div>
          <div style='font-family:Space Mono;font-size:0.6rem;
                      color:#64748b;margin-top:4px;'>LEO Relay · Turbine Node</div>
        </div>
        """, unsafe_allow_html=True)
        st.divider()

        st.markdown('<div class="section-hdr">UI BRIDGE</div>',
                    unsafe_allow_html=True)
        ui_host = st.text_input("Station UI host", "127.0.0.1")
        state_port = st.number_input("State port", value=9102, step=1)
        cmd_port = st.number_input("Command port", value=9103, step=1)
        sat_port = st.number_input("Satellite status port", value=9104, step=1)
        st.checkbox("Auto-start listeners", key="auto_start_listeners")
        if st.session_state["auto_start_listeners"] and not st.session_state["listeners_started"]:
            _start_udp_listeners(int(state_port), int(sat_port))
            st.session_state["listeners_started"] = True
        if st.button("Connect listeners", use_container_width=True):
            if not st.session_state["listeners_started"]:
                _start_udp_listeners(int(state_port), int(sat_port))
                st.session_state["listeners_started"] = True
        st.caption(f"Listeners running: {st.session_state['listeners_started']}")
        with _state_lock:
            if _listener_error:
                st.session_state["listener_error"] = _listener_error
        if st.session_state.get("listener_error"):
            st.error(st.session_state["listener_error"])
        if st.button("Refresh now", use_container_width=True):
            _rerun()
        with _state_lock:
            last_state = _state_last_rx
            state_count = _state_rx_count
        with _sat_lock:
            last_sat = _sat_last_rx
            sat_count = _sat_rx_count
        st.markdown('<div class="section-hdr">LISTENER HEALTH</div>',
                    unsafe_allow_html=True)
        st.write(f"State packets: {state_count}")
        st.write(f"Satellite packets: {sat_count}")
        st.write(f"State last RX: {time.strftime('%H:%M:%S', time.localtime(last_state)) if last_state else '—'}")
        st.write(f"Sat last RX: {time.strftime('%H:%M:%S', time.localtime(last_sat)) if last_sat else '—'}")

        st.divider()
        st.markdown('<div class="section-hdr">DIAGNOSTICS</div>',
                    unsafe_allow_html=True)
        ui_cfg, cfg_path = _read_config_ui()
        if ui_cfg is not None:
            st.write(f"Config file: {cfg_path}")
            st.write(f"ui.enabled: {ui_cfg.get('enabled')}")
            st.write(f"ui.host: {ui_cfg.get('host')}")
            st.write(f"ui.state_port: {ui_cfg.get('state_port')}")
            st.write(f"ui.satellite_status_port: {ui_cfg.get('satellite_status_port')}")
        else:
            st.write("Config file: not readable")

        if st.button("Send test packet to state port", use_container_width=True):
            _send_test_packet("127.0.0.1", int(state_port), {"_test": "state", "ts": time.time()})
        if st.button("Send test packet to sat port", use_container_width=True):
            _send_test_packet("127.0.0.1", int(sat_port), {"_test": "sat", "ts": time.time()})

        st.divider()
        st.markdown('<div class="section-hdr">CONTROL</div>',
                    unsafe_allow_html=True)
        yaw_val = st.slider("Yaw (°)", 0.0, 359.0, 0.0, 1.0)
        if st.button("Send Yaw", use_container_width=True):
            _send_ui_cmd(ui_host, int(cmd_port), "yaw", float(yaw_val))
        pitch_val = st.slider("Pitch (°)", 0.0, 90.0, 0.0, 0.5)
        if st.button("Send Pitch", use_container_width=True):
            _send_ui_cmd(ui_host, int(cmd_port), "pitch", float(pitch_val))
        if st.button("Clear Fault", use_container_width=True):
            _send_ui_cmd(ui_host, int(cmd_port), "clear")
        if st.button("Inject Fault", use_container_width=True):
            _send_ui_cmd(ui_host, int(cmd_port), "fault")

    with _state_lock:
        state = dict(_state_data)
    with _sat_lock:
        sat = dict(_sat_data)

    st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=DM+Sans:wght@300;400;500;600&display=swap');

    :root {
        --teal:    #00c4b4;
        --blue:    #0066ff;
        --orange:  #ff6b35;
        --red:     #ff3b3b;
        --green:   #00e676;
        --yellow:  #ffd600;
        --bg:      #0a0e1a;
        --surface: #111827;
        --border:  #1e2d40;
        --text:    #e2e8f0;
        --muted:   #64748b;
    }

    html, body, [data-testid="stAppViewContainer"] {
        background-color: var(--bg) !important;
        color: var(--text) !important;
        font-family: 'DM Sans', sans-serif;
    }

    [data-testid="stSidebar"] {
        background-color: #0d1220 !important;
        border-right: 1px solid var(--border);
    }

    [data-testid="stHeader"] { background: transparent !important; }

    h1, h2, h3 { font-family: 'Space Mono', monospace !important; }

    [data-testid="metric-container"] {
        background: var(--surface);
        border: 1px solid var(--border);
        border-radius: 8px;
        padding: 12px 16px;
    }

    [data-testid="stMetricValue"] {
        font-family: 'Space Mono', monospace !important;
        color: var(--teal) !important;
        font-size: 1.6rem !important;
    }

    [data-testid="stMetricLabel"] {
        color: var(--muted) !important;
        font-size: 0.75rem !important;
        text-transform: uppercase;
        letter-spacing: 0.08em;
    }

    .stButton > button {
        background: var(--surface) !important;
        border: 1px solid var(--teal) !important;
        color: var(--teal) !important;
        font-family: 'Space Mono', monospace !important;
        font-size: 0.75rem !important;
        border-radius: 4px !important;
        transition: all 0.2s;
    }
    .stButton > button:hover {
        background: var(--teal) !important;
        color: var(--bg) !important;
    }

    [data-testid="stSlider"] label {
        color: var(--text) !important;
        font-size: 0.8rem !important;
    }

    [data-testid="stSelectbox"] label { color: var(--muted) !important; font-size: 0.75rem !important; }
    .stSelectbox > div > div {
        background: var(--surface) !important;
        border: 1px solid var(--border) !important;
        color: var(--text) !important;
    }

    .section-hdr {
        font-family: 'Space Mono', monospace;
        font-size: 0.65rem;
        letter-spacing: 0.15em;
        text-transform: uppercase;
        color: var(--muted);
        border-bottom: 1px solid var(--border);
        padding-bottom: 6px;
        margin-bottom: 12px;
    }

    .live-dot {
        display:inline-block; width:8px; height:8px;
        background:var(--green); border-radius:50%;
        animation: pulse 1.5s infinite;
        margin-right:6px;
    }
    .dead-dot {
        display:inline-block; width:8px; height:8px;
        background:var(--red); border-radius:50%;
        margin-right:6px;
    }
    @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.3} }
    </style>
    """, unsafe_allow_html=True)

    live_status = bool(state)
    dot = "<span class='live-dot'></span>" if live_status else "<span class='dead-dot'></span>"
    st.markdown(
        f"<h2 style='margin:0;font-size:1.1rem;color:#e2e8f0;'>"
        f"{dot}WTSP Control Station — Live Operations Dashboard</h2>"
        f"<div style='font-family:Space Mono;font-size:0.65rem;color:#64748b;"
        f"margin-bottom:16px;'>LEO relay · turbine node · {'ONLINE' if live_status else 'OFFLINE'}</div>",
        unsafe_allow_html=True
    )

    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Link", "UP" if state.get("link_up") else "DOWN")
    k2.metric("RTT (ms)", state.get("rtt_ms") or "—")
    k3.metric("TX", state.get("msgs_tx", 0))
    k4.metric("RX", state.get("msgs_rx", 0))
    k5.metric("Handshake", "OK" if state.get("handshake_ok") else "PENDING")

    sensor = state.get("sensor", {})
    telem = state.get("telemetry", {})
    alarms = state.get("alarms", [])
    events = state.get("events", [])

    if state:
        _push_history(state)

    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "Telemetry and Power",
        "Temps and Vibration",
        "Satellite and Channel",
        "Security",
        "Events and Logs",
    ])

    with tab1:
        st.markdown('<div class="section-hdr">LIVE TELEMETRY</div>',
                    unsafe_allow_html=True)
        t1, t2, t3, t4 = st.columns(4)
        t1.metric("State", sensor.get("state", "—"))
        t1.metric("Wind (m/s)", sensor.get("wind_speed_ms", "—"))
        t2.metric("Rotor RPM", sensor.get("rotor_rpm", "—"))
        t2.metric("Power (kW)", sensor.get("power_kw", "—"))
        t3.metric("Nacelle °C", sensor.get("nacelle_temp_c", "—"))
        t3.metric("Vibration (g)", sensor.get("vibration_g", "—"))
        t4.metric("Yaw (deg)", sensor.get("yaw_deg", "—"))
        t4.metric("Pitch (deg)", sensor.get("pitch_deg", "—"))
        t4.metric("Uptime (s)", telem.get("uptime_s", "—"))

        st.markdown('<div class="section-hdr">TRENDS</div>',
                    unsafe_allow_html=True)
        h = st.session_state["hist"]
        c1, c2 = st.columns(2)
        with c1:
            st.plotly_chart(
                _line_chart(h["time"], {"Power (kW)": h["power"]}, height=180,
                            colors=["#00c4b4"]),
                use_container_width=True
            )
        with c2:
            st.plotly_chart(
                _line_chart(h["time"], {"Wind (m/s)": h["wind"]}, height=180,
                            colors=["#0066ff"]),
                use_container_width=True
            )

        c3, c4 = st.columns(2)
        with c3:
            st.plotly_chart(
                _line_chart(h["time"], {"RPM": h["rpm"]}, height=140,
                            colors=["#ff6b35"]),
                use_container_width=True
            )
        with c4:
            st.plotly_chart(
                _line_chart(h["time"], {"RTT (ms)": h["rtt"]}, height=140,
                            colors=["#ffd600"]),
                use_container_width=True
            )

    with tab2:
        st.markdown('<div class="section-hdr">CONDITION MONITORING</div>',
                    unsafe_allow_html=True)
        c1, c2 = st.columns(2)
        with c1:
            st.plotly_chart(
                _line_chart(st.session_state["hist"]["time"],
                            {"Nacelle °C": st.session_state["hist"]["temp"]},
                            height=180, colors=["#ff6b35"]),
                use_container_width=True
            )
        with c2:
            st.plotly_chart(
                _line_chart(st.session_state["hist"]["time"],
                            {"Vibration (g)": st.session_state["hist"]["vib"]},
                            height=180, colors=["#ff3b3b"]),
                use_container_width=True
            )

        st.markdown('<div class="section-hdr">ALARMS</div>',
                    unsafe_allow_html=True)
        if alarms:
            st.json(alarms)
        else:
            st.write("No active alarms")

    with tab3:
        st.markdown('<div class="section-hdr">SATELLITE / CHANNEL STATUS</div>',
                    unsafe_allow_html=True)
        st.json(sat or {})

    with tab4:
        st.markdown('<div class="section-hdr">SECURITY ALERTS</div>',
                    unsafe_allow_html=True)
        sec = [a for a in (alarms or []) if str(a.get("code", "")).startswith("SECURITY_")]
        if sec:
            st.json(sec)
        else:
            st.info("No security alerts.")

    with tab5:
        st.markdown('<div class="section-hdr">PROTOCOL EVENTS</div>',
                    unsafe_allow_html=True)
        if events:
            st.code("\n".join(events[:25]))
        else:
            st.write("No events yet")

        st.markdown('<div class="section-hdr">RUDP STATS</div>',
                    unsafe_allow_html=True)
        rs1, rs2 = st.columns(2)
        rs1.code(state.get("rudp_status", "—"))
        rs2.json(state.get("rudp_stats", {}))

        st.markdown('<div class="section-hdr">VIDEO (BONUS TASK 1)</div>',
                    unsafe_allow_html=True)
        vin = state.get("video_in", {})
        vout = state.get("video_out", {})
        c1, c2 = st.columns(2)
        c1.write("Downlink (turbine → station)")
        c1.code(json.dumps(vin, indent=2) if vin else "—")
        c2.write("Uplink (station → turbine)")
        c2.code(json.dumps(vout, indent=2) if vout else "—")

    if st.session_state.get("auto_refresh", True):
        time.sleep(0.5)
        _rerun()


if __name__ == "__main__":
    main()
