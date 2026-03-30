import json
import socket
import threading
import time

import streamlit as st

_state_lock = threading.Lock()
_sat_lock = threading.Lock()
_state_data = {}
_sat_data = {}
_listener_error = None


def _rerun():
    try:
        st.rerun()
    except AttributeError:
        st.experimental_rerun()


def _start_udp_listeners(state_port: int, sat_port: int, max_size: int = 65535):
    def _loop_state():
        global _listener_error
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

    def _loop_sat():
        global _listener_error
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

    threading.Thread(target=_loop_state, daemon=True).start()
    threading.Thread(target=_loop_sat, daemon=True).start()


def _send_ui_cmd(host: str, port: int, cmd: str, value=None):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    payload = {"cmd": cmd}
    if value is not None:
        payload["value"] = value
    sock.sendto(json.dumps(payload).encode("utf-8"), (host, port))


def main():
    st.set_page_config(page_title="WTSP Control Station", layout="wide")
    if "listeners_started" not in st.session_state:
        st.session_state["listeners_started"] = False
    if "listener_error" not in st.session_state:
        st.session_state["listener_error"] = ""
    st.sidebar.checkbox("Auto-refresh", value=True, key="auto_refresh")

    st.title("WTSP Control Station — Streamlit Demo")

    with st.sidebar:
        st.header("UI Bridge")
        ui_host = st.text_input("Station UI host", "127.0.0.1")
        state_port = st.number_input("State port", value=9102, step=1)
        cmd_port = st.number_input("Command port", value=9103, step=1)
        sat_port = st.number_input("Satellite status port", value=9104, step=1)
        if st.button("Connect listeners"):
            if not st.session_state["listeners_started"]:
                _start_udp_listeners(int(state_port), int(sat_port))
                st.session_state["listeners_started"] = True
        st.caption(f"Listeners running: {st.session_state['listeners_started']}")
        with _state_lock:
            if _listener_error:
                st.session_state["listener_error"] = _listener_error
        if st.session_state.get("listener_error"):
            st.error(st.session_state["listener_error"])
        if st.button("Refresh now"):
            _rerun()

    with _state_lock:
        state = dict(_state_data)
    with _sat_lock:
        sat = dict(_sat_data)

    st.markdown(
        """
        <style>
        .panel { background: #f7f5f2; border: 1px solid #e2ddd6; border-radius: 14px; padding: 16px; }
        .title { font-family: "Palatino", "Georgia", serif; letter-spacing: 0.5px; }
        .subtitle { color: #5d5d5d; }
        .kpi { font-size: 22px; font-weight: 700; }
        .kpi-label { color: #666; font-size: 12px; text-transform: uppercase; letter-spacing: 1px; }
        .soft { color: #777; }
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.markdown("<h1 class='title'>WTSP Control Station</h1>", unsafe_allow_html=True)
    st.markdown("<div class='subtitle'>Wind Turbine Space Protocol — Live Operations Dashboard</div>", unsafe_allow_html=True)

    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Link", "UP" if state.get("link_up") else "DOWN")
    k2.metric("RTT (ms)", state.get("rtt_ms") or "—")
    k3.metric("TX", state.get("msgs_tx", 0))
    k4.metric("RX", state.get("msgs_rx", 0))
    k5.metric("Handshake", "OK" if state.get("handshake_ok") else "PENDING")

    st.subheader("Telemetry")
    sensor = state.get("sensor", {})
    telem = state.get("telemetry", {})
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

    st.subheader("Controls")
    c1, c2, c3, c4 = st.columns(4)
    yaw_val = c1.number_input("Yaw", value=0.0)
    pitch_val = c2.number_input("Pitch", value=0.0)
    if c3.button("Set Yaw"):
        _send_ui_cmd(ui_host, int(cmd_port), "yaw", float(yaw_val))
    if c3.button("Set Pitch"):
        _send_ui_cmd(ui_host, int(cmd_port), "pitch", float(pitch_val))
    if c4.button("Clear Fault"):
        _send_ui_cmd(ui_host, int(cmd_port), "clear")
    if c4.button("Inject Fault"):
        _send_ui_cmd(ui_host, int(cmd_port), "fault")

    st.subheader("Alarms")
    alarms = state.get("alarms", [])
    if alarms:
        st.json(alarms)
    else:
        st.write("No active alarms")

    st.subheader("Protocol Events")
    events = state.get("events", [])
    if events:
        st.code("\n".join(events[:20]))
    else:
        st.write("No events yet")

    st.subheader("RUDP Stats")
    rs1, rs2 = st.columns(2)
    rs1.code(state.get("rudp_status", "—"))
    rs2.json(state.get("rudp_stats", {}))

    st.subheader("Satellite / Channel Status")
    st.json(sat or {})

    st.subheader("Video (Bonus Task 1)")
    vin = state.get("video_in", {})
    vout = state.get("video_out", {})
    c1, c2 = st.columns(2)
    c1.write("Downlink (turbine → station)")
    c1.code(json.dumps(vin, indent=2) if vin else "—")
    c2.write("Uplink (station → turbine)")
    c2.code(json.dumps(vout, indent=2) if vout else "—")

    st.subheader("Security (Bonus Task 3)")
    sec = [a for a in (state.get("alarms", []) or []) if str(a.get("code", "")).startswith("SECURITY_")]
    if sec:
        st.json(sec)
    else:
        st.info("No security alerts.")

    if st.session_state.get("auto_refresh", True):
        time.sleep(0.5)
        _rerun()


if __name__ == "__main__":
    main()
