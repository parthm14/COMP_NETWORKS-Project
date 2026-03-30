"""
Wind Turbine Node  —  Machine 1
=================================
Transport: raw UDP sockets + our own Stop-and-Wait ARQ (no TCP).

Four independent UDP services, each bound to its own port:
  7001  Yaw control      — receives CONTROL_CMD(set_yaw),   replies CONTROL_ACK
  7002  Pitch control    — receives CONTROL_CMD(set_pitch),  replies CONTROL_ACK
  7003  Sensor stream    — periodically sends SENSOR_DATA    (unreliable/fire-and-forget)
  7004  Telemetry        — sends TELEMETRY + ALARM; handles HELLO/NEGOTIATE/AGREE

All four use ReliableUDP (ARQ) except 7003 which is intentionally
fire-and-forget (best-effort sensor stream, as is common in SCADA systems).

Usage:
    python3 turbine/turbine.py [--config path/to/config.json]
"""

import sys
import os
import json
import math
import random
import threading
import time
import logging
import argparse

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from common.protocol import Message, MsgType, NodeID, AlarmLevel, make_nack
from common.reliable_udp import ReliableUDP

logging.basicConfig(
    level=logging.INFO,
    format='[TURBINE %(asctime)s] %(levelname)s  %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger('turbine')


# ══════════════════════════════════════════════
#  Security monitor (bonus task 3)
# ══════════════════════════════════════════════
class SecurityMonitor:
    def __init__(self, threshold: int = 3, quarantine_s: float = 5.0):
        self._lock = threading.Lock()
        self._score = {}         # addr -> score
        self._blocked_until = {} # addr -> monotonic time
        self._last_seq = {}      # addr -> last seq
        self._last_cmd_at = {}   # addr -> monotonic time
        self._alerts = []        # list of dicts to forward as ALARM
        self._threshold = threshold
        self._quarantine_s = quarantine_s

    def is_blocked(self, addr: tuple) -> bool:
        with self._lock:
            until = self._blocked_until.get(addr, 0)
        return time.monotonic() < until

    def record_event(self, addr: tuple, code: str, message: str):
        with self._lock:
            self._alerts.append({
                "code": f"SECURITY_{code}",
                "level": int(AlarmLevel.CRITICAL),
                "message": message,
                "ts": time.time(),
                "src": f"{addr[0]}:{addr[1]}",
            })
            self._score[addr] = self._score.get(addr, 0) + 1
            if self._score[addr] >= self._threshold:
                self._blocked_until[addr] = time.monotonic() + self._quarantine_s

    def check_replay(self, addr: tuple, seq_num: int) -> bool:
        with self._lock:
            last = self._last_seq.get(addr)
            self._last_seq[addr] = seq_num
        if last is None:
            return False
        return seq_num <= last

    def check_rate(self, addr: tuple, min_interval_s: float = 0.2) -> bool:
        now = time.monotonic()
        with self._lock:
            last = self._last_cmd_at.get(addr)
            self._last_cmd_at[addr] = now
        if last is None:
            return False
        return (now - last) < min_interval_s

    def pop_alerts(self):
        with self._lock:
            alerts = list(self._alerts)
            self._alerts.clear()
            return alerts


security = SecurityMonitor()


# ══════════════════════════════════════════════
#  Physical model — simulates a 5 MW offshore turbine
# ══════════════════════════════════════════════
class TurbineModel:
    """
    Background thread updates all sensor values every second using
    realistic physics (Ornstein-Uhlenbeck wind, cubic power curve, etc.).
    Actuators (yaw, pitch) move toward their targets at realistic rates.
    """

    class State:
        IDLE     = "IDLE"
        STARTING = "STARTING"
        RUNNING  = "RUNNING"
        FAULT    = "FAULT"

    # 5 MW offshore turbine parameters
    CUT_IN_MS    = 3.0
    CUT_OUT_MS   = 25.0
    RATED_WIND   = 12.0
    RATED_POWER  = 5000.0   # kW
    RATED_RPM    = 15.0
    ROTOR_RADIUS = 63.0     # metres

    def __init__(self):
        self.state           = self.State.IDLE
        self._lock           = threading.Lock()

        # Sensors
        self.wind_ms         = 8.0
        self.rotor_rpm       = 0.0
        self.power_kw        = 0.0
        self.nacelle_temp_c  = 25.0
        self.vibration_g     = 0.02
        self.energy_kwh      = 0.0
        self.uptime_s        = 0

        # Actuators  (current position)
        self.yaw_deg         = 0.0
        self.pitch_deg       = 0.0
        # Actuator targets  (set by control commands)
        self._target_yaw     = 0.0
        self._target_pitch   = 0.0

        # Active alarms (list of dicts)
        self.alarms          = []

        self._running = True
        threading.Thread(target=self._loop, daemon=True).start()

    # ── Simulation loop ──────────────────────
    def _loop(self):
        while self._running:
            time.sleep(1)
            self.uptime_s += 1
            self._step_wind()
            self._step_state()
            self._step_sensors()
            self._step_actuators()
            self._step_faults()

    def _step_wind(self):
        """Ornstein-Uhlenbeck mean-reverting wind speed."""
        self.wind_ms += 0.05 * (10.0 - self.wind_ms) + 0.5 * random.gauss(0, 1)
        self.wind_ms  = max(0.0, min(35.0, self.wind_ms))

    def _step_state(self):
        w = self.wind_ms
        with self._lock:
            s = self.state
        if s == self.State.IDLE and w >= self.CUT_IN_MS:
            self._set_state(self.State.STARTING)
        elif s == self.State.STARTING and self.rotor_rpm >= 2.0:
            self._set_state(self.State.RUNNING)
        elif s == self.State.RUNNING and w < self.CUT_IN_MS:
            self._set_state(self.State.IDLE)
        elif s == self.State.RUNNING and w > self.CUT_OUT_MS:
            self._add_alarm("HIGH_WIND", AlarmLevel.CRITICAL,
                            f"Wind {w:.1f} m/s exceeds cut-out {self.CUT_OUT_MS} m/s")
            self._set_state(self.State.FAULT)

    def _step_sensors(self):
        with self._lock:
            s = self.state
        if s == self.State.RUNNING:
            target_rpm = min(self.RATED_RPM,
                             self.RATED_RPM * self.wind_ms / self.RATED_WIND)
            self.rotor_rpm += 0.5 * (target_rpm - self.rotor_rpm)

            cp    = 0.45
            rho   = 1.225
            area  = math.pi * self.ROTOR_RADIUS ** 2
            pitch_factor = max(0.05, self._target_pitch / 90.0)
            self.power_kw = min(
                self.RATED_POWER,
                0.5 * cp * rho * area * self.wind_ms**3 / 1000.0 * pitch_factor
            )
            self.energy_kwh += self.power_kw / 3600.0

        elif s == self.State.STARTING:
            self.rotor_rpm = min(self.rotor_rpm + 0.3, 3.0)
            self.power_kw  = 0.0
        else:
            self.rotor_rpm = max(0.0, self.rotor_rpm - 0.5)
            self.power_kw  = 0.0

        self.nacelle_temp_c += (
            0.01 * self.power_kw / 500.0
            - 0.05 * (self.nacelle_temp_c - 20.0)
        )
        self.nacelle_temp_c  = max(15.0, min(90.0, self.nacelle_temp_c))
        self.vibration_g     = 0.01 + 0.002 * self.rotor_rpm + random.gauss(0, 0.005)

    def _step_actuators(self):
        # Yaw: 1°/s
        diff = (self._target_yaw - self.yaw_deg + 540) % 360 - 180
        self.yaw_deg = (self.yaw_deg + max(-1.0, min(1.0, diff))) % 360
        # Pitch: 2°/s
        diff = self._target_pitch - self.pitch_deg
        self.pitch_deg = max(0.0, min(90.0,
                                      self.pitch_deg + max(-2.0, min(2.0, diff))))

    def _step_faults(self):
        if self.nacelle_temp_c > 75.0:
            self._add_alarm("HIGH_TEMP", AlarmLevel.WARNING,
                            f"Nacelle {self.nacelle_temp_c:.1f}°C")
        if self.vibration_g > 0.15:
            self._add_alarm("HIGH_VIB", AlarmLevel.WARNING,
                            f"Vibration {self.vibration_g:.3f}g")
        # Auto-clear resolved alarms
        self.alarms = [a for a in self.alarms if not self._resolved(a)]

    def _resolved(self, alarm):
        c = alarm["code"]
        return (
            (c == "HIGH_TEMP" and self.nacelle_temp_c < 70.0) or
            (c == "HIGH_VIB"  and self.vibration_g    < 0.10) or
            (c == "HIGH_WIND" and self.wind_ms         < 20.0)
        )

    # ── Internal helpers ──────────────────────
    def _set_state(self, s):
        with self._lock:
            self.state = s
        log.info(f"State → {s}")

    def _add_alarm(self, code, level, message):
        if not any(a["code"] == code for a in self.alarms):
            self.alarms.append({
                "code": code, "level": int(level),
                "message": message, "ts": time.time()
            })
            log.warning(f"ALARM {code}: {message}")

    # ── Public command API ────────────────────
    def cmd_yaw(self, degrees: float) -> str:
        self._target_yaw = float(degrees) % 360
        log.info(f"Yaw target → {self._target_yaw:.1f}°")
        return f"Yaw target set to {self._target_yaw:.1f}°"

    def cmd_pitch(self, degrees: float) -> str:
        with self._lock:
            if self.state == self.State.FAULT:
                return "REJECTED: turbine in FAULT state"
        self._target_pitch = max(0.0, min(90.0, float(degrees)))
        log.info(f"Pitch target → {self._target_pitch:.1f}°")
        return f"Pitch target set to {self._target_pitch:.1f}°"

    def cmd_clear_fault(self) -> str:
        with self._lock:
            if self.state == self.State.FAULT:
                self.state = self.State.IDLE
                self.alarms.clear()
                log.info("Fault cleared by station command")
                return "Fault cleared"
        return "No active fault"

    def cmd_inject_fault(self, reason: str = "MANUAL_INJECT") -> str:
        with self._lock:
            self.state = self.State.FAULT
        self._add_alarm(
            reason,
            AlarmLevel.CRITICAL,
            "Fault injected for demo/verification"
        )
        log.warning("Fault injected by station command")
        return "Fault injected"
    def snapshot(self) -> dict:
        with self._lock:
            s = self.state
        return {
            "state":          s,
            "wind_speed_ms":  round(self.wind_ms, 2),
            "rotor_rpm":      round(self.rotor_rpm, 2),
            "power_kw":       round(self.power_kw, 1),
            "nacelle_temp_c": round(self.nacelle_temp_c, 1),
            "vibration_g":    round(self.vibration_g, 4),
            "yaw_deg":        round(self.yaw_deg, 1),
            "pitch_deg":      round(self.pitch_deg, 1),
            "energy_kwh":     round(self.energy_kwh, 2),
            "uptime_s":       self.uptime_s,
            "alarms":         list(self.alarms),
        }

    def stop(self):
        self._running = False


# ══════════════════════════════════════════════
#  Service 1 — Yaw Control  (UDP port 7001, reliable)
# ══════════════════════════════════════════════
class YawService:
    """
    Receives CONTROL_CMD(set_yaw) via ReliableUDP.
    Replies with CONTROL_ACK (the ACK is sent automatically by ReliableUDP;
    we also send a richer CONTROL_ACK with the confirmed position).
    """
    def __init__(self, host, port, turbine: TurbineModel, sat_addr: tuple, rudp_kwargs=None):
        self.turbine  = turbine
        self.sat_addr = sat_addr    # we send ACKs back toward the satellite
        self.rudp = ReliableUDP(NodeID.TURBINE, host, port, **(rudp_kwargs or {}))
        self.rudp.on_message = self._handle

    def start(self):
        self.rudp.start()
        log.info(f"Yaw control   UDP :{self.rudp.port}")

    def _handle(self, msg: Message, addr: tuple):
        if msg.msg_type != MsgType.CONTROL_CMD:
            return
        if security.is_blocked(addr):
            log.warning(f"[SEC] blocked sender {addr} (yaw)")
            return
        if security.check_replay(addr, msg.seq_num):
            log.warning(f"[SEC] replay detected from {addr} (yaw)")
            security.record_event(addr, "REPLAY", "Replay detected on yaw command")
            nack = make_nack(msg, NodeID.TURBINE, "replay detected")
            self.rudp.send_unreliable(nack, addr)
            return
        if security.check_rate(addr):
            log.warning(f"[SEC] rate limit from {addr} (yaw)")
            security.record_event(addr, "RATE", "Command rate too high (yaw)")
            nack = make_nack(msg, NodeID.TURBINE, "rate limit")
            self.rudp.send_unreliable(nack, addr)
            return
        data   = msg.payload_json() or {}
        log.info(f"[CTRL] set_yaw from {addr}")
        try:
            val = float(data.get("value", 0))
        except Exception:
            security.record_event(addr, "MALFORMED", "Non-numeric yaw value")
            nack = make_nack(msg, NodeID.TURBINE, "invalid yaw value")
            self.rudp.send_unreliable(nack, addr)
            return
        if abs(val) > 720:
            security.record_event(addr, "RANGE", "Yaw value out of safe range")
            nack = make_nack(msg, NodeID.TURBINE, "yaw out of range")
            self.rudp.send_unreliable(nack, addr)
            return
        result = self.turbine.cmd_yaw(val)

        ack = Message(
            MsgType.CONTROL_ACK, NodeID.TURBINE, msg.src,
            payload={"result": result,
                     "yaw_deg":   round(self.turbine.yaw_deg, 1),
                     "target":    self.turbine._target_yaw}
        )
        # Send result back the way the message came (through satellite)
        self.rudp.send_reliable(ack, addr)


# ══════════════════════════════════════════════
#  Service 2 — Pitch Control  (UDP port 7002, reliable)
# ══════════════════════════════════════════════
class PitchService:
    """
    Receives CONTROL_CMD(set_pitch / clear_fault) via ReliableUDP.
    """
    def __init__(self, host, port, turbine: TurbineModel, rudp_kwargs=None):
        self.turbine = turbine
        self.rudp = ReliableUDP(NodeID.TURBINE, host, port, **(rudp_kwargs or {}))
        self.rudp.on_message = self._handle

    def start(self):
        self.rudp.start()
        log.info(f"Pitch control UDP :{self.rudp.port}")

    def _handle(self, msg: Message, addr: tuple):
        if msg.msg_type != MsgType.CONTROL_CMD:
            return
        if security.is_blocked(addr):
            log.warning(f"[SEC] blocked sender {addr} (pitch)")
            return
        if security.check_replay(addr, msg.seq_num):
            log.warning(f"[SEC] replay detected from {addr} (pitch)")
            security.record_event(addr, "REPLAY", "Replay detected on pitch/clear command")
            nack = make_nack(msg, NodeID.TURBINE, "replay detected")
            self.rudp.send_unreliable(nack, addr)
            return
        if security.check_rate(addr):
            log.warning(f"[SEC] rate limit from {addr} (pitch)")
            security.record_event(addr, "RATE", "Command rate too high (pitch)")
            nack = make_nack(msg, NodeID.TURBINE, "rate limit")
            self.rudp.send_unreliable(nack, addr)
            return
        data   = msg.payload_json() or {}
        action = data.get("action", "set_pitch")
        log.info(f"[CTRL] {action} from {addr}")

        if action == "clear_fault":
            result = self.turbine.cmd_clear_fault()
        elif action == "inject_fault":
            result = self.turbine.cmd_inject_fault()
        elif action == "set_pitch":
            try:
                val = float(data.get("value", 0))
            except Exception:
                security.record_event(addr, "MALFORMED", "Non-numeric pitch value")
                nack = make_nack(msg, NodeID.TURBINE, "invalid pitch value")
                self.rudp.send_unreliable(nack, addr)
                return
            if val < 0 or val > 90:
                security.record_event(addr, "RANGE", "Pitch value out of safe range")
                nack = make_nack(msg, NodeID.TURBINE, "pitch out of range")
                self.rudp.send_unreliable(nack, addr)
                return
            result = self.turbine.cmd_pitch(val)
        else:
            security.record_event(addr, "MALFORMED", f"Unknown control action {action}")
            nack = make_nack(msg, NodeID.TURBINE, "unknown control action")
            self.rudp.send_unreliable(nack, addr)
            return

        ack = Message(
            MsgType.CONTROL_ACK, NodeID.TURBINE, msg.src,
            payload={"result": result,
                     "pitch_deg": round(self.turbine.pitch_deg, 1),
                     "target":    self.turbine._target_pitch}
        )
        self.rudp.send_reliable(ack, addr)


# ══════════════════════════════════════════════
#  Service 3 — Sensor Stream  (UDP port 7003, unreliable)
# ══════════════════════════════════════════════
class SensorStream:
    """
    Periodically broadcasts SENSOR_DATA datagrams to the satellite uplink.
    UDP fire-and-forget — no ACK, no retransmit.
    The satellite applies channel loss; the station sees what arrives.
    """
    def __init__(self, host, port, dest_addr: tuple,
                 turbine: TurbineModel, interval: float = 2.0, rudp_kwargs=None):
        self.dest_addr = dest_addr
        self.turbine   = turbine
        self.interval  = interval
        self.rudp      = ReliableUDP(NodeID.TURBINE, host, port, **(rudp_kwargs or {}))

    def start(self):
        self.rudp.start()
        threading.Thread(target=self._run, daemon=True).start()
        log.info(f"Sensor stream UDP :{self.rudp.port} → {self.dest_addr[0]}:{self.dest_addr[1]}")

    def _run(self):
        while True:
            time.sleep(self.interval)
            msg = Message(
                MsgType.SENSOR_DATA, NodeID.TURBINE, NodeID.STATION,
                payload=self.turbine.snapshot()
            )
            self.rudp.send_unreliable(msg, self.dest_addr)


# ══════════════════════════════════════════════
#  Service 4 — Telemetry  (UDP port 7004, reliable)
# ══════════════════════════════════════════════
class TelemetryService:
    """
    Handles:
      • HELLO / NEGOTIATE / AGREE  (discovery handshake)
      • HEARTBEAT / HEARTBEAT_ACK  (keep-alive)
      Sends:
      • TELEMETRY  (periodic health report, pushed to station)
      • ALARM      (when faults occur)

    Because UDP is connectionless, we don't "push" to a connection —
    instead the turbine remembers the station's address from the last
    HELLO it received, then sends telemetry there.
    """
    def __init__(self, host, port, turbine: TurbineModel, interval: float = 5.0, rudp_kwargs=None):
        self.turbine       = turbine
        self.interval      = interval
        self.rudp          = ReliableUDP(NodeID.TURBINE, host, port, **(rudp_kwargs or {}))
        self.rudp.on_message = self._handle

        self._station_addr  = None      # set when HELLO arrives
        self._addr_lock     = threading.Lock()
        self._last_alarms   = set()

    def start(self):
        self.rudp.start()
        threading.Thread(target=self._push_loop, daemon=True).start()
        log.info(f"Telemetry     UDP :{self.rudp.port}")

    def _handle(self, msg: Message, addr: tuple):
        # Remember where the station is
        with self._addr_lock:
            self._station_addr = addr

        if msg.msg_type == MsgType.HELLO:
            if security.is_blocked(addr):
                log.warning(f"[SEC] blocked sender {addr} (hello)")
                return
            if msg.payload_json() is None:
                security.record_event(addr, "MALFORMED", "HELLO payload invalid JSON")
                nack = make_nack(msg, NodeID.TURBINE, "invalid HELLO payload")
                self.rudp.send_unreliable(nack, addr)
                return
            log.info(f"[TELEM] HELLO from {addr}")
            reply = Message(
                MsgType.HELLO_ACK, NodeID.TURBINE, msg.src,
                payload={
                    "node_id":  "TURBINE_G1",
                    "version":  "1.0",
                    "services": ["yaw:7001", "pitch:7002",
                                 "sensor:7003", "telemetry:7004",
                                 "video:7005"],
                    "location": {"lat": 53.5, "lon": -6.3,
                                 "desc": "Irish Sea, offshore"}
                }
            )
            self.rudp.send_reliable(reply, addr)

        elif msg.msg_type == MsgType.NEGOTIATE:
            if security.is_blocked(addr):
                log.warning(f"[SEC] blocked sender {addr} (negotiate)")
                return
            payload = msg.payload_json()
            if payload is None:
                security.record_event(addr, "MALFORMED", "NEGOTIATE payload invalid JSON")
                nack = make_nack(msg, NodeID.TURBINE, "invalid NEGOTIATE payload")
                self.rudp.send_unreliable(nack, addr)
                return
            interval = payload.get("requested_sensor_interval_s", 2)
            sensors = payload.get("requested_sensors", [])
            if not isinstance(sensors, list) or len(sensors) > 10:
                security.record_event(addr, "NEGOTIATE", "Suspicious sensors list")
                nack = make_nack(msg, NodeID.TURBINE, "invalid sensors list")
                self.rudp.send_unreliable(nack, addr)
                return
            if interval < 1 or interval > 10:
                security.record_event(addr, "NEGOTIATE", "Suspicious interval request")
                nack = make_nack(msg, NodeID.TURBINE, "invalid interval")
                self.rudp.send_unreliable(nack, addr)
                return
            log.info("[TELEM] NEGOTIATE received")
            reply = Message(
                MsgType.NEGOTIATE_ACK, NodeID.TURBINE, msg.src,
                payload={
                    "capabilities": {
                        "sensor_interval_s":    2,
                        "telemetry_interval_s": 5,
                        "yaw_rate_deg_s":       1.0,
                        "pitch_rate_deg_s":     2.0,
                        "rated_power_kw":       5000,
                    },
                    "constraints": {
                        "max_wind_cut_out_ms": 25.0,
                        "pitch_range_deg":     [0.0, 90.0],
                        "yaw_range_deg":       [0.0, 360.0],
                    }
                }
            )
            self.rudp.send_reliable(reply, addr)

        elif msg.msg_type == MsgType.AGREE:
            data = msg.payload_json() or {}
            log.info(f"[TELEM] AGREE received — plan: {data.get('plan')}")

        elif msg.msg_type == MsgType.HEARTBEAT:
            hb_ack = Message(MsgType.HEARTBEAT_ACK, NodeID.TURBINE, msg.src)
            self.rudp.send_reliable(hb_ack, addr)

    def _push_loop(self):
        """Periodically push TELEMETRY and any new ALARMs to station."""
        while True:
            time.sleep(self.interval)
            with self._addr_lock:
                addr = self._station_addr
            if addr is None:
                continue    # no station connected yet

            snap = self.turbine.snapshot()

            # ── Push telemetry ──
            telem = Message(
                MsgType.TELEMETRY, NodeID.TURBINE, NodeID.STATION,
                payload={
                    "state":      snap["state"],
                    "uptime_s":   snap["uptime_s"],
                    "energy_kwh": snap["energy_kwh"],
                    "temp_c":     snap["nacelle_temp_c"],
                    "vibration":  snap["vibration_g"],
                    "alarm_count": len(snap["alarms"]),
                }
            )
            ok = self.rudp.send_reliable(telem, addr)
            if not ok:
                log.warning("[TELEM] Could not deliver telemetry to station")

            # ── Push new alarms ──
            current_codes = {a["code"] for a in snap["alarms"]}
            new_codes     = current_codes - self._last_alarms
            self._last_alarms = current_codes

            for alarm in snap["alarms"]:
                if alarm["code"] in new_codes:
                    alarm_msg = Message(
                        MsgType.ALARM, NodeID.TURBINE, NodeID.STATION,
                        payload=alarm
                    )
                    self.rudp.send_reliable(alarm_msg, addr)
                    log.warning(f"[TELEM] Alarm pushed: {alarm['code']}")

            # ── Push security alerts ──
            for alert in security.pop_alerts():
                sec_msg = Message(
                    MsgType.ALARM, NodeID.TURBINE, NodeID.STATION,
                    payload=alert
                )
                self.rudp.send_reliable(sec_msg, addr)
                log.warning(f"[SEC] Alert pushed: {alert['code']}")


# ══════════════════════════════════════════════
#  Service 5 — Video/Frame Stream (UDP port 7005, unreliable)
# ══════════════════════════════════════════════
class VideoStreamService:
    """
    Bi-directional O+M frame stream.
    Outbound frames are sent unreliably to the satellite uplink.
    Inbound frames from station are accepted for demo realism.
    """
    def __init__(self, host, port, dest_addr: tuple, turbine: TurbineModel,
                 interval: float = 1.0, rudp_kwargs=None):
        self.dest_addr = dest_addr
        self.turbine = turbine
        self.interval = interval
        self.rudp = ReliableUDP(NodeID.TURBINE, host, port, **(rudp_kwargs or {}))
        self.rudp.on_message = self._handle
        self._seq = 0

    def start(self):
        self.rudp.start()
        threading.Thread(target=self._run, daemon=True).start()
        log.info(f"Video stream  UDP :{self.rudp.port} → {self.dest_addr[0]}:{self.dest_addr[1]}")

    def _handle(self, msg: Message, addr: tuple):
        if msg.msg_type != MsgType.VIDEO_FRAME:
            return
        payload = msg.payload_json() or {}
        log.info(f"[VIDEO] RX frame {payload.get('seq')} from station")

    def _run(self):
        while True:
            time.sleep(self.interval)
            snap = self.turbine.snapshot()
            self._seq += 1
            frame = {
                "seq": self._seq,
                "ts": time.time(),
                "src": "turbine_cam",
                "note": "O+M nacelle cam",
                "yaw_deg": snap.get("yaw_deg"),
                "pitch_deg": snap.get("pitch_deg"),
                "power_kw": snap.get("power_kw"),
                "state": snap.get("state"),
            }
            msg = Message(MsgType.VIDEO_FRAME, NodeID.TURBINE, NodeID.STATION, payload=frame)
            self.rudp.send_unreliable(msg, self.dest_addr)


# ══════════════════════════════════════════════
#  Main
# ══════════════════════════════════════════════
def main():
    parser = argparse.ArgumentParser(description="WTSP Wind Turbine Node (UDP)")
    parser.add_argument('--config', default='config.json')
    args = parser.parse_args()

    cfg_path = os.path.join(os.path.dirname(__file__), '..', args.config)
    with open(cfg_path) as f:
        cfg = json.load(f)

    turbine_host  = cfg["turbine"]["host"]
    ports         = cfg["turbine"]["ports"]
    sat_host      = cfg["satellite"]["host"]
    sat_ports     = cfg["satellite"]["ports"]
    proto_cfg     = cfg["protocol"]
    rudp_kwargs   = {
        "timeout_s": proto_cfg["ack_timeout_s"],
        "max_retry": proto_cfg["max_retries"],
    }

    # Satellite uplink address (turbine sends sensor data and telemetry here)
    sat_turbine_uplink = (sat_host, sat_ports["uplink_from_turbine"])

    turbine = TurbineModel()

    YawService(turbine_host,   ports["yaw_control"],   turbine, sat_turbine_uplink, rudp_kwargs=rudp_kwargs).start()
    PitchService(turbine_host, ports["pitch_control"],  turbine, rudp_kwargs=rudp_kwargs).start()
    SensorStream(turbine_host, ports["sensor_stream"],  sat_turbine_uplink, turbine, rudp_kwargs=rudp_kwargs).start()
    TelemetryService(turbine_host, ports["telemetry"],  turbine, rudp_kwargs=rudp_kwargs).start()
    if "video_stream" in ports:
        VideoStreamService(turbine_host, ports["video_stream"], sat_turbine_uplink,
                           turbine, interval=1.0, rudp_kwargs=rudp_kwargs).start()

    log.info("=" * 58)
    log.info("  Wind Turbine node ready  (all UDP, custom ARQ)")
    log.info(f"  Yaw     : UDP {turbine_host}:{ports['yaw_control']}")
    log.info(f"  Pitch   : UDP {turbine_host}:{ports['pitch_control']}")
    log.info(f"  Sensor  : UDP {turbine_host}:{ports['sensor_stream']} → sat")
    log.info(f"  Telem   : UDP {turbine_host}:{ports['telemetry']}")
    log.info("=" * 58)

    try:
        while True:
            time.sleep(10)
            s = turbine.snapshot()
            log.info(
                f"STATE={s['state']:8s}  wind={s['wind_speed_ms']:5.1f}m/s  "
                f"rpm={s['rotor_rpm']:5.1f}  power={s['power_kw']:7.1f}kW  "
                f"yaw={s['yaw_deg']:5.1f}°  pitch={s['pitch_deg']:4.1f}°"
            )
    except KeyboardInterrupt:
        log.info("Turbine node shutdown.")
        turbine.stop()


if __name__ == '__main__':
    main()
