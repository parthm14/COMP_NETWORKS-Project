"""
Control Station Node  —  Machine 3  (Space Station)
=====================================================
Transport: raw UDP sockets + our own Stop-and-Wait ARQ.  No TCP.

Architecture:
  • One ReliableUDP socket on port 9001 handles BOTH:
      - inbound traffic from turbine via satellite
      - outbound reliable/unreliable traffic to the satellite
  • Discovery handshake (HELLO → HELLO_ACK → NEGOTIATE → NEGOTIATE_ACK
    → AGREE) runs once on startup over the telemetry channel.
  • Heartbeat is sent every 10 s; RTT is measured from HEARTBEAT_ACK.

Usage:
    python3 control_station/station.py [--config path/to/config.json]

Interactive commands (type while dashboard is running):
    yaw <deg>      — set turbine nacelle yaw target
    pitch <deg>    — set blade pitch target
    clear          — send fault-clear command
    fault          — inject a FAULT for demo verification
    status         — dump full JSON snapshot
    quit           — exit
"""

import sys
import os
import json
import threading
import time
import logging
import argparse
from collections import deque
import socket

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from common.protocol import Message, MsgType, NodeID
from common.reliable_udp import ReliableUDP

logging.basicConfig(
    level=logging.INFO,
    format='[STATION %(asctime)s] %(levelname)s  %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger('station')

# Prevent dashboard refresh from overwriting interactive CLI input.
input_active = threading.Event()

# Throttle noisy video RX logs so CLI input stays readable.
_last_video_log_ts = 0.0


# ══════════════════════════════════════════════
#  Shared state  (written by receiver thread, read by dashboard)
# ══════════════════════════════════════════════
class StationState:
    def __init__(self, link_timeout_s: float = 15.0):
        self._lock        = threading.Lock()
        self.sensor       = {}           # latest SENSOR_DATA payload
        self.telemetry    = {}           # latest TELEMETRY payload
        self.alarms       = []           # active alarm dicts
        self.turbine_caps = {}           # from NEGOTIATE_ACK
        self.rtt_ms       = None
        self.msgs_rx      = 0
        self.msgs_tx      = 0
        self.link_timeout_s = link_timeout_s
        self.last_rx_at   = None
        self.last_tx_at   = None
        self.handshake_ok = False
        self.last_command_ack = {}
        self.events = deque(maxlen=200)
        self.video_in = {}
        self.video_out = {}

    def add_event(self, msg: str):
        ts = time.strftime("%H:%M:%S")
        with self._lock:
            self.events.appendleft(f"{ts} {msg}")

    def _mark_rx_unlocked(self):
        self.last_rx_at = time.monotonic()
        self.msgs_rx += 1

    def set_sensor(self, d):
        with self._lock:
            self.sensor = d
            self._mark_rx_unlocked()

    def set_telemetry(self, d):
        with self._lock:
            self.telemetry = d
            if d.get("alarm_count", 0) == 0:
                self.alarms.clear()
            self._mark_rx_unlocked()

    def add_alarm(self, a):
        with self._lock:
            if a.get("code") not in {x.get("code") for x in self.alarms}:
                self.alarms.append(a)
            self._mark_rx_unlocked()

    def clear_alarms(self):
        with self._lock:
            self.alarms.clear()

    def clear_alarm_code(self, code: str):
        with self._lock:
            self.alarms = [a for a in self.alarms if a.get("code") != code]

    def tx(self):
        with self._lock:
            self.msgs_tx += 1
            self.last_tx_at = time.monotonic()

    def note_rx(self):
        with self._lock:
            self._mark_rx_unlocked()

    def set_handshake_ok(self, ok: bool):
        with self._lock:
            self.handshake_ok = ok

    def set_command_ack(self, d):
        with self._lock:
            self.last_command_ack = dict(d)
            if "yaw_deg" in d:
                self.sensor["yaw_deg"] = d["yaw_deg"]
            if "pitch_deg" in d:
                self.sensor["pitch_deg"] = d["pitch_deg"]
            self._mark_rx_unlocked()

    def snapshot(self):
        with self._lock:
            now = time.monotonic()
            link_up = (
                self.last_rx_at is not None and
                (now - self.last_rx_at) <= self.link_timeout_s
            )
            return dict(
                sensor=dict(self.sensor),
                telemetry=dict(self.telemetry),
                alarms=list(self.alarms),
                turbine_caps=dict(self.turbine_caps),
                rtt_ms=self.rtt_ms,
                msgs_rx=self.msgs_rx,
                msgs_tx=self.msgs_tx,
                handshake_ok=self.handshake_ok,
                link_up=link_up,
                last_command_ack=dict(self.last_command_ack),
                events=list(self.events),
                video_in=dict(self.video_in),
                video_out=dict(self.video_out),
            )


# ══════════════════════════════════════════════
#  Inbound listener  — one UDP port handles all arriving traffic
# ══════════════════════════════════════════════
class InboundListener:
    """
    Binds to station:9001.
    All traffic from the turbine (via satellite) arrives here:
      SENSOR_DATA, TELEMETRY, ALARM, CONTROL_ACK, HEARTBEAT_ACK,
      HELLO_ACK, NEGOTIATE_ACK, ACK
    """
    def __init__(self, host: str, port: int, state: StationState,
                 my_node: NodeID = NodeID.STATION, rudp_kwargs=None):
        self.state = state
        self.rudp  = ReliableUDP(my_node, host, port, **(rudp_kwargs or {}))
        self.rudp.on_message = self._dispatch

        # Events set by handshake (Commander waits on these)
        self.hello_ack_event     = threading.Event()
        self.negotiate_ack_event = threading.Event()

        # Stored handshake replies
        self.hello_ack_data      = {}
        self.negotiate_ack_data  = {}

        # For RTT measurement: set just before sending HEARTBEAT
        self._hb_sent_at = None

    def start(self):
        self.rudp.start()
        log.info(f"[NET ] Inbound listener UDP :{self.rudp.port}")

    def _dispatch(self, msg: Message, addr: tuple):
        if msg.msg_type != MsgType.ACK:
            with self.state._lock:
                last = self.state.last_rx_at
                timeout_s = self.state.link_timeout_s
            if last is None or (time.monotonic() - last) > timeout_s:
                log.info("[LINK] Recovery — inbound traffic resumed")
                self.state.add_event("Link recovered (traffic resumed)")

        t = msg.msg_type

        if t == MsgType.SENSOR_DATA:
            d = msg.payload_json() or {}
            self.state.set_sensor(d)

        elif t == MsgType.TELEMETRY:
            d = msg.payload_json() or {}
            self.state.set_telemetry(d)
            log.debug(f"[TELEM] {d}")

        elif t == MsgType.ALARM:
            d = msg.payload_json() or {}
            self.state.add_alarm(d)
            log.info(f"[ALARM] {d.get('code')}: {d.get('message')}")
            self.state.add_event(f"Alarm raised: {d.get('code')}")
            if str(d.get("code", "")).startswith("SECURITY_"):
                self.state.add_event("Security: alert received")

        elif t == MsgType.HELLO_ACK:
            self.hello_ack_data = msg.payload_json() or {}
            self.state.note_rx()
            log.info(f"[PROTO] HELLO_ACK from turbine: {self.hello_ack_data}")
            self.state.add_event("Discovery: HELLO_ACK")
            self.hello_ack_event.set()

        elif t == MsgType.NEGOTIATE_ACK:
            d = msg.payload_json() or {}
            with self.state._lock:
                self.state.turbine_caps = d.get("capabilities", {})
                self.state._mark_rx_unlocked()
            self.negotiate_ack_data = d
            log.info(f"[PROTO] NEGOTIATE_ACK caps={self.state.turbine_caps}")
            self.state.add_event("Negotiation: NEGOTIATE_ACK")
            self.negotiate_ack_event.set()

        elif t == MsgType.HEARTBEAT_ACK:
            self.state.note_rx()
            if self._hb_sent_at:
                rtt = (time.monotonic() - self._hb_sent_at) * 1000
                with self.state._lock:
                    self.state.rtt_ms = round(rtt, 1)
                self._hb_sent_at = None
                log.debug(f"[HB] RTT={rtt:.1f}ms")

        elif t == MsgType.CONTROL_ACK:
            d = msg.payload_json() or {}
            self.state.set_command_ack(d)
            log.info(f"[CTRL] CMD_ACK {d}")
            self.state.add_event("Control: CMD_ACK")
            if (d.get("result") or "").lower().startswith("fault cleared"):
                self.state.clear_alarm_code("MANUAL_INJECT")
        elif t == MsgType.VIDEO_FRAME:
            d = msg.payload_json() or {}
            with self.state._lock:
                self.state.video_in = d
                self.state._mark_rx_unlocked()
            now = time.time()
            global _last_video_log_ts
            if not input_active.is_set() and (now - _last_video_log_ts) > 2.0:
                _last_video_log_ts = now
                log.info(f"[VIDEO] RX frame {d.get('seq')} from turbine")
            self.state.add_event("Video: RX frame")

        elif t == MsgType.ACK:
            pass    # handled internally by ReliableUDP

        else:
            log.debug(f"Unhandled msg type {t.name} from {addr}")


# ══════════════════════════════════════════════
#  Commander — sends control commands to turbine via satellite
# ══════════════════════════════════════════════
class Commander:
    """
    Runs the discovery handshake then exposes yaw/pitch/clear_fault/inject_fault.
    Commands travel:  Station → Satellite:8001 → Turbine:70xx
    Replies travel:   Turbine → Satellite:8002 → Station:9001

    IMPORTANT:
    We send USING THE SAME ReliableUDP SOCKET bound to port 9001.
    That keeps the station as one consistent endpoint and makes ARQ sane.
    """
    def __init__(self,
                 sat_addr: tuple,
                 state: StationState,
                 listener: InboundListener,
                 my_node: NodeID = NodeID.STATION):
        self.sat_addr  = sat_addr
        self.state     = state
        self.listener  = listener
        self.my_node   = my_node
        self._send_lock = threading.Lock()

    def _send_reliable(self, msg: Message) -> bool:
        with self._send_lock:
            self.state.tx()
            return self.listener.rudp.send_reliable(msg, self.sat_addr)

    def _send_unreliable(self, msg: Message) -> None:
        with self._send_lock:
            self.state.tx()
            self.listener.rudp.send_unreliable(msg, self.sat_addr)

    def handshake(self, timeout: float = 15.0):
        """HELLO → NEGOTIATE → AGREE — run once at startup."""
        self.listener.hello_ack_event.clear()
        self.listener.negotiate_ack_event.clear()
        self.state.set_handshake_ok(False)

        # HELLO
        hello = Message(
            MsgType.HELLO,
            self.my_node,
            NodeID.TURBINE,
            payload={"node_id": "STATION", "version": "1.0"}
        )
        log.info(f"[PROTO] HELLO → satellite {self.sat_addr}")
        self.state.add_event("Discovery: HELLO sent")
        if not self._send_reliable(hello):
            log.warning("[RUDP] HELLO delivery failed")
            return False

        if not self.listener.hello_ack_event.wait(timeout=timeout):
            log.warning("[PROTO] HELLO_ACK timeout (satellite may be in outage)")
            return False

        # NEGOTIATE
        neg = Message(
            MsgType.NEGOTIATE,
            self.my_node,
            NodeID.TURBINE,
            payload={
                "requested_sensor_interval_s": 2,
                "requested_sensors": ["wind", "rpm", "power", "temp", "vib"],
            }
        )
        self.state.add_event("Negotiation: NEGOTIATE sent")
        if not self._send_reliable(neg):
            log.warning("[RUDP] NEGOTIATE delivery failed")
            return False

        if not self.listener.negotiate_ack_event.wait(timeout=timeout):
            log.warning("[PROTO] NEGOTIATE_ACK timeout")
            return False

        # AGREE
        agree = Message(
            MsgType.AGREE,
            self.my_node,
            NodeID.TURBINE,
            payload={"agreed": True, "plan": "periodic_telemetry_2s"}
        )
        self.state.add_event("Agreement: AGREE sent")
        if not self._send_reliable(agree):
            log.warning("[RUDP] AGREE delivery failed")
            return False

        self.state.set_handshake_ok(True)
        log.info("[PROTO] Handshake complete — link established")
        self.state.add_event("Agreement: Handshake complete")
        return True

    def set_yaw(self, degrees: float) -> bool:
        msg = Message(
            MsgType.CONTROL_CMD,
            self.my_node,
            NodeID.TURBINE,
            payload={"action": "set_yaw", "value": round(float(degrees), 1)}
        )
        log.info(f"[CTRL] CMD_YAW → {degrees}°")
        self.state.add_event(f"Control: set_yaw {degrees}")
        return self._send_reliable(msg)

    def set_pitch(self, degrees: float) -> bool:
        msg = Message(
            MsgType.CONTROL_CMD,
            self.my_node,
            NodeID.TURBINE,
            payload={"action": "set_pitch", "value": round(float(degrees), 1)}
        )
        log.info(f"[CTRL] CMD_PITCH → {degrees}°")
        self.state.add_event(f"Control: set_pitch {degrees}")
        return self._send_reliable(msg)

    def clear_fault(self) -> bool:
        msg = Message(
            MsgType.CONTROL_CMD,
            self.my_node,
            NodeID.TURBINE,
            payload={"action": "clear_fault"}
        )
        log.info("[CTRL] CMD_CLEAR_FAULT")
        self.state.add_event("Control: clear_fault")
        return self._send_reliable(msg)

    def inject_fault(self) -> bool:
        msg = Message(
            MsgType.CONTROL_CMD,
            self.my_node,
            NodeID.TURBINE,
            payload={"action": "inject_fault"}
        )
        log.warning("[CTRL] CMD_INJECT_FAULT")
        self.state.add_event("Control: inject_fault")
        return self._send_reliable(msg)

    def send_heartbeat(self):
        hb = Message(MsgType.HEARTBEAT, self.my_node, NodeID.TURBINE)
        self.listener._hb_sent_at = time.monotonic()
        self._send_unreliable(hb)


# ══════════════════════════════════════════════
#  Heartbeat loop
# ══════════════════════════════════════════════
class HeartbeatLoop:
    def __init__(self, commander: Commander, interval: float = 10.0):
        self.commander = commander
        self.interval  = interval

    def start(self):
        threading.Thread(target=self._run, daemon=True).start()

    def _run(self):
        while True:
            time.sleep(self.interval)
            self.commander.send_heartbeat()


class VideoSender:
    """
    Periodically sends lightweight O+M frames from the station to the turbine.
    Uses the station's existing RUDP socket (unreliable send).
    """
    def __init__(self, commander: Commander, state: StationState, interval: float = 1.5):
        self.commander = commander
        self.state = state
        self.interval = interval
        self._seq = 0

    def start(self):
        threading.Thread(target=self._run, daemon=True).start()

    def _run(self):
        while True:
            time.sleep(self.interval)
            self._seq += 1
            frame = {
                "seq": self._seq,
                "ts": time.time(),
                "src": "station_cam",
                "note": "O+M inspection uplink",
            }
            msg = Message(MsgType.VIDEO_FRAME, NodeID.STATION, NodeID.TURBINE, payload=frame)
            self.commander._send_unreliable(msg)
            with self.state._lock:
                self.state.video_out = dict(frame)
            self.state.add_event("Video: TX frame")

class UiBridge:
    """
    Lightweight UDP bridge for Streamlit UI.
    Receives UI commands and periodically broadcasts station state.
    """
    def __init__(self, state: StationState, commander: Commander, listener: InboundListener,
                 host: str, state_port: int, cmd_port: int, interval_s: float = 1.0):
        self.state = state
        self.commander = commander
        self.listener = listener
        self.host = host
        self.state_port = state_port
        self.cmd_port = cmd_port
        self.interval_s = interval_s

        self._tx_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._rx_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._rx_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # Bind to 0.0.0.0 so Streamlit (same machine) can reach this on any interface
        self._rx_sock.bind(('0.0.0.0', cmd_port))

    def start(self):
        threading.Thread(target=self._tx_loop, daemon=True).start()
        threading.Thread(target=self._rx_loop, daemon=True).start()
        log.info(f"[UI  ] bridge active: state→{self.host}:{self.state_port} cmd←{self.host}:{self.cmd_port}")

    def _build_snapshot(self):
        snap = self.state.snapshot()
        snap["rudp_stats"] = dict(self.listener.rudp.stats)
        snap["rudp_status"] = self.listener.rudp.status()
        return snap

    def _tx_loop(self):
        while True:
            time.sleep(self.interval_s)
            payload = json.dumps(self._build_snapshot(), separators=(',', ':')).encode("utf-8")
            self._tx_sock.sendto(payload, (self.host, self.state_port))

    def _rx_loop(self):
        while True:
            data, _ = self._rx_sock.recvfrom(4096)
            try:
                msg = json.loads(data.decode("utf-8"))
            except Exception:
                continue
            cmd = msg.get("cmd")
            if cmd == "yaw":
                self.commander.set_yaw(float(msg.get("value", 0)))
            elif cmd == "pitch":
                self.commander.set_pitch(float(msg.get("value", 0)))
            elif cmd == "clear":
                self.commander.clear_fault()
            elif cmd == "fault":
                self.commander.inject_fault()
            else:
                log.info(f"UI cmd ignored: {msg}")

def handshake_until_success(commander: Commander, timeout: float = 30.0, retry_delay: float = 5.0):
    while True:
        if commander.handshake(timeout=timeout):
            return
        log.warning(f"[PROTO] Handshake incomplete; retrying in {retry_delay:.0f}s")
        time.sleep(retry_delay)


# ══════════════════════════════════════════════
#  Live Dashboard
# ══════════════════════════════════════════════
class Dashboard:
    def __init__(self, state: StationState, interval: float = 2.0):
        self.state    = state
        self.interval = interval

    def start(self):
        threading.Thread(target=self._run, daemon=True).start()

    def _run(self):
        while True:
            time.sleep(self.interval)
            self._draw()

    def _draw(self):
        if input_active.is_set():
            return
        s   = self.state.snapshot()
        t   = s["sensor"]
        tel = s["telemetry"]
        lnk = "UP" if s["link_up"] else "DOWN"
        rtt = f"{s['rtt_ms']} ms" if s["rtt_ms"] else "—"
        hs  = "OK" if s["handshake_ok"] else "PENDING"
        ts  = time.strftime("%H:%M:%S")
        events = s.get("events", [])
        proto_events = [e for e in events if e.startswith("Discovery") or e.startswith("Negotiation") or e.startswith("Agreement") or e.startswith("Control")]
        proto_tail = proto_events[-3:] if proto_events else []

        def fnum(val, fmt="{:.2f}"):
            return fmt.format(val) if isinstance(val, (int, float)) else "—"

        lines = [
            "\033[2J\033[H",
            "WTSP Control Station — Live Status",
            f"Time: {ts}  Link: {lnk}  Handshake: {hs}  RTT: {rtt}  TX/RX: {s['msgs_tx']}/{s['msgs_rx']}",
            "-" * 78,
            "Protocol Lifecycle",
        ]
        if proto_tail:
            for e in proto_tail:
                lines.append(f"  - {e}")
        else:
            lines.append("  - (waiting for HELLO/NEGOTIATE/AGREE)")
        lines += [
            "Turbine Sensors",
            f"  State: {t.get('state','—'):<8}  Wind: {fnum(t.get('wind_speed_ms'), '{:.2f}')} m/s  "
            f"RPM: {fnum(t.get('rotor_rpm'), '{:.2f}')}  Power: {fnum(t.get('power_kw'), '{:.1f}')} kW",
            f"  Nacelle: {fnum(t.get('nacelle_temp_c'), '{:.1f}')} °C  "
            f"Vibration: {fnum(t.get('vibration_g'), '{:.4f}')} g",
            "Actuators",
            f"  Yaw: {fnum(t.get('yaw_deg'), '{:.1f}')}°  Pitch: {fnum(t.get('pitch_deg'), '{:.1f}')}°",
            "Telemetry",
            f"  Uptime: {t.get('uptime_s','—')} s  Energy: {fnum(tel.get('energy_kwh'), '{:.2f}')} kWh",
            "Alarms",
        ]

        alarms = s["alarms"]
        if alarms:
            for a in alarms[:3]:
                code = a.get("code", "?")
                msg  = a.get("message", "")[:60]
                lines.append(f"  - {code}: {msg}")
        else:
            lines.append("  - None")

        lines += [
            "-" * 78,
            "Video Feed (RX)",
            f"  Last frame: {s.get('video_in', {}).get('seq', '—')}  "
            f"Last note: {s.get('video_in', {}).get('note', '—')}",
            "-" * 78,
            "Enter command: yaw <deg> | pitch <deg> | clear | fault | status",
            "",
        ]
        print("\n".join(lines), end='', flush=True)


# ══════════════════════════════════════════════
#  Interactive CLI
# ══════════════════════════════════════════════
def cli_loop(commander: Commander, state: StationState):
    print("\nControl Station ready.  Type commands below:")
    while True:
        try:
            input_active.set()
            raw = input(">>> ").strip()
            input_active.clear()
        except (EOFError, KeyboardInterrupt):
            input_active.clear()
            break
        if not raw:
            continue

        parts = raw.split()
        cmd   = parts[0].lower()

        if cmd in ("quit", "exit"):
            break
        elif cmd == "yaw" and len(parts) == 2:
            try:
                commander.set_yaw(float(parts[1]))
            except ValueError:
                print("Usage: yaw <degrees>")
        elif cmd == "pitch" and len(parts) == 2:
            try:
                commander.set_pitch(float(parts[1]))
            except ValueError:
                print("Usage: pitch <degrees>")
        elif cmd == "clear":
            commander.clear_fault()
        elif cmd == "fault":
            commander.inject_fault()
        elif cmd == "status":
            print(json.dumps(state.snapshot(), indent=2, default=str))
        else:
            print(f"Unknown: {raw}  (yaw / pitch / clear / status / quit)")


# ══════════════════════════════════════════════
#  Main
# ══════════════════════════════════════════════
def main():
    parser = argparse.ArgumentParser(description="WTSP Control Station (UDP)")
    parser.add_argument('--config', default='config.json')
    args = parser.parse_args()

    cfg_path = os.path.join(os.path.dirname(__file__), '..', args.config)
    with open(cfg_path) as f:
        cfg = json.load(f)

    sta_host  = cfg["station"]["host"]
    sta_ports = cfg["station"]["ports"]
    sat_host  = cfg["satellite"]["host"]
    sat_ports = cfg["satellite"]["ports"]

    proto_cfg = cfg["protocol"]
    rudp_kwargs = {
        "timeout_s": proto_cfg["ack_timeout_s"],
        "max_retry": proto_cfg["max_retries"],
    }

    state    = StationState(link_timeout_s=max(12.0, proto_cfg["heartbeat_interval"] * 2.5))
    listener = InboundListener(
        sta_host,
        sta_ports["data_receiver"],
        state,
        rudp_kwargs=rudp_kwargs,
    )
    listener.start()

    sat_addr  = (sat_host, sat_ports["uplink_from_station"])
    commander = Commander(sat_addr, state, listener)

    # Start heartbeat loop
    HeartbeatLoop(commander, interval=proto_cfg["heartbeat_interval"]).start()
    # Start video uplink stream (bonus task 1)
    VideoSender(commander, state, interval=1.5).start()

    # Start live dashboard
    Dashboard(state).start()

    log.info("=" * 56)
    log.info("[WTSP] Control Station started  (UDP + custom ARQ)")
    log.info(f"[NET ] Inbound RX : {sta_host}:{sta_ports['data_receiver']}")
    log.info(f"[NET ] Satellite  : {sat_host}:{sat_ports['uplink_from_station']}")
    log.info("=" * 56)

    # Optional UI bridge for Streamlit
    ui_cfg = cfg.get("ui", {})
    if ui_cfg.get("enabled", False):
        UiBridge(
            state,
            commander,
            listener,
            host=ui_cfg.get("host", "127.0.0.1"),
            state_port=ui_cfg.get("state_port", 9102),
            cmd_port=ui_cfg.get("command_port", 9103),
            interval_s=ui_cfg.get("state_interval_s", 1.0),
        ).start()

    # Run discovery handshake in background so CLI can start immediately
    threading.Thread(
        target=lambda: handshake_until_success(commander, timeout=30, retry_delay=5),
        daemon=True
    ).start()

    # CLI blocks in main thread
    cli_loop(commander, state)
    log.info("Control station shutdown.")


if __name__ == '__main__':
    main()
