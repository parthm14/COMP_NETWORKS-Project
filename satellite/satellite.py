"""
LEO Satellite Relay Node  —  Machine 2
========================================
Transport: raw UDP sockets only. No TCP.

The satellite has two listening UDP ports:
  8001  — uplink FROM the control station  (station → turbine direction)
  8002  — uplink FROM the wind turbine     (turbine → station direction)

For every datagram received it:
  1. Validates the WTSP magic + CRC
  2. Consults the channel simulator (orbital window, loss, delay)
  3. If not dropped: sleeps delay_s, then forwards the raw datagram

Important forwarding rule:
  - station → turbine packets must be forwarded out from satellite port 8002
  - turbine → station packets must be forwarded out from satellite port 8001

ACK routing fix:
  When a turbine-originated reliable message is forwarded to the station,
  the satellite stores:
      seq_num -> turbine source port
  Then when the station later sends ACK(ack_seq) back, the satellite routes
  that ACK to the same turbine UDP service port that originated the message.

Usage:
    python3 satellite/satellite.py [--config path/to/config.json]
"""

import sys
import os
import json
import socket
import threading
import time
import logging
import argparse

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from common.protocol import Message, MsgType, MIN_MSG_LEN
from common.channel import ChannelSimulator

logging.basicConfig(
    level=logging.INFO,
    format='[SAT %(asctime)s] %(levelname)s  %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger('satellite')


# ──────────────────────────────────────────────
#  ACK route tracker
# ──────────────────────────────────────────────
class AckRouteTable:
    """
    Stores mapping:
        seq_num -> turbine source address

    Used so that ACKs coming back from the station can be forwarded to the
    exact turbine UDP service endpoint that originated the reliable message.
    """
    def __init__(self, max_entries: int = 2048):
        self._lock = threading.Lock()
        self._map = {}
        self._order = []
        self._max_entries = max_entries

    def remember(self, seq_num: int, turbine_src_addr: tuple):
        with self._lock:
            if seq_num not in self._map:
                self._order.append(seq_num)
            self._map[seq_num] = turbine_src_addr

            while len(self._order) > self._max_entries:
                old = self._order.pop(0)
                self._map.pop(old, None)

    def lookup(self, seq_num: int):
        with self._lock:
            return self._map.get(seq_num)


# ──────────────────────────────────────────────
#  Routing table: msg_type → turbine port key
# ──────────────────────────────────────────────
def _turbine_port_for(msg: Message, ports: dict) -> int:
    """Choose which turbine port to forward a station→turbine message to."""
    if msg.msg_type == MsgType.CONTROL_CMD:
        payload = msg.payload_json() or {}
        action = payload.get("action", "")
        if action == "set_yaw":
            return ports["yaw_control"]       # 7001
        elif action in ("set_pitch", "clear_fault", "inject_fault"):
            return ports["pitch_control"]     # 7002
    if msg.msg_type == MsgType.VIDEO_FRAME:
        return ports.get("video_stream", ports["telemetry"])

    # HELLO / NEGOTIATE / AGREE / HEARTBEAT / fallback
    return ports["telemetry"]                 # 7004


# ──────────────────────────────────────────────
#  UDP relay pipeline
# ──────────────────────────────────────────────
class UdpRelay:
    """
    Listens on one UDP port, applies channel effects, forwards datagrams.

    One instance handles one direction of traffic.
    """

    def __init__(
        self,
        name: str,
        channel: ChannelSimulator,
        listen_host: str,
        listen_port: int
    ):
        self.name = name
        self.channel = channel
        self.listen_host = listen_host
        self.listen_port = listen_port

        # Bind to 0.0.0.0 so this socket accepts packets arriving on any
        # network interface (WiFi, hotspot, etc.) on this machine.
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind(('0.0.0.0', listen_port))

        self.stats = {
            "rx": 0,
            "fwd": 0,
            "drop_channel": 0,
            "drop_parse": 0,
        }
        self._last_drop_log = 0.0
        self._drop_log_interval_s = 5.0

    def start(self, forward_fn):
        self._forward_fn = forward_fn
        threading.Thread(
            target=self._recv_loop,
            daemon=True,
            name=self.name
        ).start()
        log.info(f"[NET ] {self.name} listening on {self.listen_host}:{self.listen_port} (UDP)")

    def _recv_loop(self):
        while True:
            try:
                raw, src_addr = self._sock.recvfrom(65535)
            except OSError:
                break

            self.stats["rx"] += 1

            if len(raw) < MIN_MSG_LEN:
                self.stats["drop_parse"] += 1
                continue

            try:
                msg = Message.unpack(raw)
            except ValueError as e:
                self.stats["drop_parse"] += 1
                log.debug(f"[{self.name}] bad datagram: {e}")
                continue

            delay_s, dropped = self.channel.process(len(raw))

            if dropped:
                self.stats["drop_channel"] += 1
                reason = "outage" if not self.channel.orbit.is_visible() else "loss"
                now = time.monotonic()
                if msg.msg_type != MsgType.SENSOR_DATA or (now - self._last_drop_log) >= self._drop_log_interval_s:
                    log.info(
                        f"[{self.name}] DROP {msg.msg_type.name} "
                        f"seq={msg.seq_num} reason={reason}"
                    )
                    self._last_drop_log = now
                continue

            threading.Thread(
                target=self._delayed_forward,
                args=(raw, msg, src_addr, delay_s),
                daemon=True
            ).start()

    def _delayed_forward(self, raw, msg, src_addr, delay_s):
        time.sleep(delay_s)
        try:
            self._forward_fn(raw, msg, src_addr)
            self.stats["fwd"] += 1
            log.debug(
                f"[{self.name}] fwd {msg.msg_type.name} seq={msg.seq_num} "
                f"delay={delay_s * 1000:.1f}ms src={src_addr}"
            )
        except Exception as e:
            log.warning(f"[{self.name}] forward error: {e}")

    def raw_send(self, raw: bytes, dest_addr: tuple):
        """
        Send using this relay's bound listening socket so the forwarded packet
        keeps the correct satellite source port.
        """
        self._sock.sendto(raw, dest_addr)

    def status(self) -> str:
        s = self.stats
        return (
            f"rx={s['rx']} fwd={s['fwd']} "
            f"drop_ch={s['drop_channel']} drop_parse={s['drop_parse']}"
        )


# ──────────────────────────────────────────────
#  Status reporter
# ──────────────────────────────────────────────
class StatusReporter:
    def __init__(self, channel: ChannelSimulator, relays: list, interval: int = 15):
        self.channel = channel
        self.relays = relays
        self.interval = interval
        self._last_visible = None

    def start(self):
        threading.Thread(target=self._run, daemon=True).start()

    def _run(self):
        while True:
            time.sleep(self.interval)
            r = self.channel.status_report()
            visible = self.channel.orbit.is_visible()
            if self._last_visible is None:
                self._last_visible = visible
            elif visible != self._last_visible:
                if visible:
                    log.info("[CHANNEL] RECOVERY — satellite back in view")
                else:
                    log.info("[CHANNEL] OUTAGE — satellite below horizon")
                self._last_visible = visible
            log.info("─" * 56)
            log.info(f"[STATUS] Satellite : {r['satellite']}")
            log.info(f"[STATUS] Weather   : {r['weather']}")
            log.info(
                f"[STATUS] Channel   : loss={r['loss_pct']}  "
                f"total={r['msgs_total']}  fwd={r['msgs_fwd']}"
            )
            for relay in self.relays:
                log.info(f"[STATUS] {relay.name:10s} {relay.status()}")
            log.info("─" * 56)


class UiStatusBroadcaster:
    """
    Sends satellite/channel status snapshots over UDP for the Streamlit UI.
    """
    def __init__(self, channel: ChannelSimulator, relays: list,
                 host: str, port: int, interval_s: float = 1.0):
        self.channel = channel
        self.relays = relays
        self.host = host
        self.port = port
        self.interval_s = interval_s
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def start(self):
        threading.Thread(target=self._run, daemon=True).start()

    def _run(self):
        while True:
            time.sleep(self.interval_s)
            payload = self.channel.status_report()
            payload["relays"] = {r.name: dict(r.stats) for r in self.relays}
            raw = json.dumps(payload, separators=(',', ':')).encode("utf-8")
            self._sock.sendto(raw, (self.host, self.port))


# ──────────────────────────────────────────────
#  Main
# ──────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="WTSP LEO Satellite Relay (UDP)")
    parser.add_argument('--config', default='config.json')
    args = parser.parse_args()

    cfg_path = os.path.join(os.path.dirname(__file__), '..', args.config)
    with open(cfg_path) as f:
        cfg = json.load(f)

    sat_host = cfg["satellite"]["host"]
    sat_ports = cfg["satellite"]["ports"]
    ch_cfg = cfg["channel"]
    turb_host = cfg["turbine"]["host"]
    turb_ports = cfg["turbine"]["ports"]
    sta_host = cfg["station"]["host"]
    sta_ports = cfg["station"]["ports"]

    channel = ChannelSimulator(
        base_loss_rate=ch_cfg["packet_loss_rate"],
        bad_weather_loss=ch_cfg["bad_weather_loss_rate"],
        jitter_ms=ch_cfg["jitter_ms"],
        bandwidth_bps=ch_cfg["bandwidth_bps"],
        orbital_period_s=ch_cfg["orbital_period_s"],
        visibility_window=ch_cfg["visibility_window_s"],
    )

    ack_routes = AckRouteTable()
    station_route_lock = threading.Lock()
    station_route_addr = [None]

    # Station → Turbine
    sta_to_tur = UdpRelay(
        name="STA→TUR",
        channel=channel,
        listen_host=sat_host,
        listen_port=sat_ports["uplink_from_station"],   # 8001
    )

    # Turbine → Station
    tur_to_sta = UdpRelay(
        name="TUR→STA",
        channel=channel,
        listen_host=sat_host,
        listen_port=sat_ports["uplink_from_turbine"],   # 8002
    )

    def forward_to_turbine(raw: bytes, msg: Message, src_addr: tuple):
        """
        Forward station→turbine traffic using the 8002-bound socket.

        Special case:
          If this is an ACK/NACK coming back from the station, route it to the
          exact turbine service port that originally sent the message.
        """
        if msg.msg_type in (MsgType.ACK, MsgType.NACK):
            payload = msg.payload_json() or {}
            seq = payload.get("ack_seq")
            if seq is None:
                seq = payload.get("nack_seq")

            if seq is not None:
                remembered_addr = ack_routes.lookup(seq)
                if remembered_addr is not None:
                    tur_to_sta.raw_send(raw, remembered_addr)
                    return

        dest_port = _turbine_port_for(msg, turb_ports)
        tur_to_sta.raw_send(raw, (turb_host, dest_port))

    def forward_to_station(raw: bytes, msg: Message, src_addr: tuple):
        """
        Forward turbine→station traffic using the 8001-bound socket.

        Before forwarding, remember which turbine source port this sequence
        number came from, so later station ACKs can be routed back correctly.
        """
        with station_route_lock:
            dest_addr = station_route_addr[0] or (sta_host, sta_ports["data_receiver"])

        if msg.msg_type != MsgType.SENSOR_DATA:
            ack_routes.remember(msg.seq_num, src_addr)

        sta_to_tur.raw_send(raw, dest_addr)

    def remember_station(raw: bytes, msg: Message, src_addr: tuple):
        with station_route_lock:
            station_route_addr[0] = src_addr
        forward_to_turbine(raw, msg, src_addr)

    sta_to_tur.start(remember_station)
    tur_to_sta.start(forward_to_station)

    StatusReporter(channel, [sta_to_tur, tur_to_sta], interval=15).start()

    ui_cfg = cfg.get("ui", {})
    if ui_cfg.get("enabled", False):
        UiStatusBroadcaster(
            channel,
            [sta_to_tur, tur_to_sta],
            host=cfg["station"]["host"],
            port=ui_cfg.get("satellite_status_port", 9104),
            interval_s=ui_cfg.get("satellite_interval_s", 1.0),
        ).start()

    log.info("=" * 56)
    log.info("[WTSP] LEO Satellite relay started (UDP only)")
    log.info(f"[NET ] Uplink from station : {sat_host}:{sat_ports['uplink_from_station']}")
    log.info(f"[NET ] Uplink from turbine : {sat_host}:{sat_ports['uplink_from_turbine']}")
    log.info(f"[NET ] → Turbine           : {turb_host}:70xx")
    log.info(f"[NET ] → Station           : {sta_host}:{sta_ports['data_receiver']}")
    log.info(f"[CHAN] Orbital period      : {ch_cfg['orbital_period_s']}s")
    log.info(f"[CHAN] Visibility window   : {ch_cfg['visibility_window_s']}s")
    log.info("=" * 56)

    try:
        while True:
            time.sleep(30)
            r = channel.status_report()
            log.info(
                f"  {r['satellite']}  |  {r['weather']}  |  loss={r['loss_pct']}"
            )
    except KeyboardInterrupt:
        log.info("Satellite relay shutdown.")


if __name__ == '__main__':
    main()
