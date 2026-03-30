"""
WTSP - Wind Turbine Space Protocol
===================================
Custom binary protocol for communication between:
  - Wind Turbine (Node 0x01)
  - LEO Satellite (Node 0x02)
  - Control Station (Node 0x03)

Transport: raw UDP sockets.  Reliability (ACK + retransmit) is implemented
           manually in common/reliable_udp.py — we do NOT use TCP.

Wire format (every message):
  [Magic:2][Version:1][Type:1][SeqNum:4][Timestamp:8][Src:1][Dst:1][PayLen:4]
                        = 22 bytes header
  [Payload: variable]
  [CRC16: 2]
                        = 24 + len(payload) bytes total

UDP preserves message boundaries so no length-prefix framing is needed.
All multi-byte fields are big-endian (network byte order, '!').
"""

import struct
import time
import json
from enum import IntEnum

# ──────────────────────────────────────────────
#  Constants
# ──────────────────────────────────────────────
MAGIC   = 0x5754        # ASCII 'WT'
VERSION = 1

# struct format string for the fixed 22-byte header
#   !  = big-endian (network order)
#   H  = unsigned short  (2) → magic
#   B  = unsigned byte   (1) → version
#   B  = unsigned byte   (1) → msg_type
#   I  = unsigned int    (4) → seq_num
#   Q  = unsigned long   (8) → timestamp_ms
#   B  = unsigned byte   (1) → src
#   B  = unsigned byte   (1) → dst
#   I  = unsigned int    (4) → payload_len
HEADER_FMT  = '!HBBIQBBI'
HEADER_SIZE = struct.calcsize(HEADER_FMT)   # = 22 bytes
CRC_SIZE    = 2
MIN_MSG_LEN = HEADER_SIZE + CRC_SIZE        # = 24 bytes


# ──────────────────────────────────────────────
#  Node identities
# ──────────────────────────────────────────────
class NodeID(IntEnum):
    TURBINE   = 0x01
    SATELLITE = 0x02
    STATION   = 0x03
    BROADCAST = 0xFF    # used for discovery messages


# ──────────────────────────────────────────────
#  Message types
# ──────────────────────────────────────────────
class MsgType(IntEnum):
    # ---- Discovery & handshake ----
    HELLO         = 0x01    # "I'm alive, who's out there?"
    HELLO_ACK     = 0x02    # "I see you"

    # ---- Capability negotiation ----
    NEGOTIATE     = 0x03    # "Here are my services/constraints"
    NEGOTIATE_ACK = 0x04    # "Accepted / here are mine"
    AGREE         = 0x05    # "We agree on the following plan"

    # ---- General transport ----
    ACK           = 0x06    # Positive acknowledgment
    NACK          = 0x07    # Negative acknowledgment (with reason)

    # ---- Turbine data (downlink: turbine → station) ----
    SENSOR_DATA   = 0x08    # Wind speed, RPM, power, temperature, vibration
    TELEMETRY     = 0x09    # Health / status report

    # ---- Control commands (uplink: station → turbine) ----
    CONTROL_CMD   = 0x0A    # Set yaw angle / blade pitch
    CONTROL_ACK   = 0x0B    # Turbine confirms command executed

    # ---- Keep-alive ----
    HEARTBEAT     = 0x0C
    HEARTBEAT_ACK = 0x0D

    # ---- Alarms ----
    ALARM         = 0x0E    # Turbine raises an alert (storm, fault, etc.)

    # ---- Routing (for multi-turbine bonus task) ----
    ROUTE_UPDATE  = 0x0F    # Share known neighbours
    VIDEO_FRAME   = 0x10    # O+M video/frame stream (bonus)


# ──────────────────────────────────────────────
#  Alarm severity levels
# ──────────────────────────────────────────────
class AlarmLevel(IntEnum):
    INFO     = 1
    WARNING  = 2
    CRITICAL = 3


# ──────────────────────────────────────────────
#  CRC-16/CCITT-FALSE  (used in space comms / HDLC)
#  Polynomial: 0x1021   Init: 0xFFFF   No reflection
# ──────────────────────────────────────────────
def crc16(data: bytes) -> int:
    crc = 0xFFFF
    for byte in data:
        crc ^= byte << 8
        for _ in range(8):
            if crc & 0x8000:
                crc = (crc << 1) ^ 0x1021
            else:
                crc <<= 1
            crc &= 0xFFFF
    return crc


# ──────────────────────────────────────────────
#  Message class
# ──────────────────────────────────────────────
class Message:
    """
    A single WTSP protocol message.

    Usage:
        # Create
        msg = Message(MsgType.SENSOR_DATA, NodeID.TURBINE, NodeID.STATION,
                      payload={"wind_speed": 12.4})
        raw = msg.pack()          # bytes to send over the socket

        # Parse
        msg2 = Message.unpack(raw)
        data = msg2.payload_json()
    """

    # A simple counter shared across all Message instances so each new
    # message automatically gets a unique, incrementing sequence number.
    _seq_counter = 0

    def __init__(self,
                 msg_type: MsgType,
                 src:      NodeID,
                 dst:      NodeID,
                 payload=b'',
                 seq_num: int = None):

        self.msg_type  = MsgType(msg_type)
        self.src       = NodeID(src)
        self.dst       = NodeID(dst)
        self.timestamp = int(time.time() * 1000)    # ms since epoch

        # Auto-increment sequence number if not provided
        if seq_num is None:
            Message._seq_counter = (Message._seq_counter + 1) & 0xFFFFFFFF
            self.seq_num = Message._seq_counter
        else:
            self.seq_num = seq_num

        # Accept dict/list → JSON bytes, or raw bytes
        if isinstance(payload, (dict, list)):
            self.payload = json.dumps(payload, separators=(',', ':')).encode('utf-8')
        elif isinstance(payload, str):
            self.payload = payload.encode('utf-8')
        else:
            self.payload = bytes(payload)

    # ---- Serialise to wire bytes ----
    def pack(self) -> bytes:
        header = struct.pack(
            HEADER_FMT,
            MAGIC,
            VERSION,
            int(self.msg_type),
            self.seq_num,
            self.timestamp,
            int(self.src),
            int(self.dst),
            len(self.payload)
        )
        body     = header + self.payload
        checksum = crc16(body)
        return body + struct.pack('!H', checksum)

    # ---- Parse from wire bytes ----
    @classmethod
    def unpack(cls, data: bytes) -> 'Message':
        if len(data) < MIN_MSG_LEN:
            raise ValueError(f"Too short ({len(data)} bytes, need ≥{MIN_MSG_LEN})")

        # 1. Verify CRC (covers header + payload, excludes the CRC bytes themselves)
        received_crc  = struct.unpack('!H', data[-CRC_SIZE:])[0]
        computed_crc  = crc16(data[:-CRC_SIZE])
        if received_crc != computed_crc:
            raise ValueError(
                f"CRC mismatch: received 0x{received_crc:04X}, "
                f"computed 0x{computed_crc:04X} — data likely corrupted"
            )

        # 2. Parse header
        (magic, version, msg_type, seq_num,
         timestamp, src, dst, payload_len) = struct.unpack(HEADER_FMT, data[:HEADER_SIZE])

        # 3. Sanity checks
        if magic != MAGIC:
            raise ValueError(f"Bad magic 0x{magic:04X} (expected 0x{MAGIC:04X})")
        if version != VERSION:
            raise ValueError(f"Unsupported version {version}")

        # 4. Extract payload
        payload = data[HEADER_SIZE: HEADER_SIZE + payload_len]

        # 5. Build object (bypass auto-increment by passing seq_num explicitly)
        msg = cls(MsgType(msg_type), NodeID(src), NodeID(dst), payload, seq_num)
        msg.timestamp = timestamp
        return msg

    # ---- Helpers ----
    def payload_json(self):
        """Return parsed JSON payload, or None if payload is not valid JSON."""
        try:
            return json.loads(self.payload.decode('utf-8'))
        except Exception:
            return None

    def age_ms(self) -> float:
        """How many milliseconds since this message was created."""
        return int(time.time() * 1000) - self.timestamp

    def __repr__(self):
        return (
            f"Message("
            f"type={self.msg_type.name}, "
            f"src={self.src.name}, "
            f"dst={self.dst.name}, "
            f"seq={self.seq_num}, "
            f"payload={len(self.payload)}B)"
        )


# ──────────────────────────────────────────────
#  Convenience constructors
# ──────────────────────────────────────────────
def make_ack(original: Message, sender: NodeID) -> Message:
    """Create an ACK for a received message."""
    return Message(
        MsgType.ACK, sender, original.src,
        payload={"ack_seq": original.seq_num},
        seq_num=original.seq_num
    )


def make_nack(original: Message, sender: NodeID, reason: str) -> Message:
    """Create a NACK for a received message."""
    return Message(
        MsgType.NACK, sender, original.src,
        payload={"nack_seq": original.seq_num, "reason": reason},
        seq_num=original.seq_num
    )


# ──────────────────────────────────────────────
#  UDP helpers  (no framing needed — UDP is already message-bounded)
# ──────────────────────────────────────────────
def udp_send(sock, msg: Message, addr: tuple) -> None:
    """Send a WTSP message as a single UDP datagram."""
    sock.sendto(msg.pack(), addr)


def udp_recv(sock, buf_size: int = 65535) -> tuple:
    """
    Receive one WTSP datagram (blocking).
    Returns (Message, sender_addr) or raises ValueError on bad data.
    """
    raw, addr = sock.recvfrom(buf_size)
    return Message.unpack(raw), addr


# ──────────────────────────────────────────────
#  Quick self-test  (run this file directly)
# ──────────────────────────────────────────────
if __name__ == '__main__':
    print("=== WTSP Protocol self-test ===\n")

    # 1. Build a SENSOR_DATA message
    msg = Message(
        MsgType.SENSOR_DATA,
        NodeID.TURBINE,
        NodeID.STATION,
        payload={
            "wind_speed_ms": 12.4,
            "rotor_rpm":     14.2,
            "power_kw":      850.0,
            "nacelle_temp_c": 38.1,
            "yaw_deg":       270.0,
            "pitch_deg":     12.0
        }
    )
    print(f"Created : {msg}")

    # 2. Pack to bytes
    raw = msg.pack()
    print(f"Packed  : {len(raw)} bytes")
    print(f"Hex     : {raw[:24].hex()} ... (header shown)")

    # 3. Unpack and verify
    msg2 = Message.unpack(raw)
    print(f"Unpacked: {msg2}")
    print(f"Payload : {msg2.payload_json()}")

    # 4. Corrupt a byte and confirm CRC catches it
    corrupted = bytearray(raw)
    corrupted[10] ^= 0xFF
    try:
        Message.unpack(bytes(corrupted))
        print("ERROR: CRC did not catch corruption!")
    except ValueError as e:
        print(f"\nCRC test passed: {e}")

    # 5. ACK helper
    ack = make_ack(msg, NodeID.STATION)
    print(f"\nACK     : {ack}")
    raw_ack = ack.pack()
    ack2 = Message.unpack(raw_ack)
    print(f"ACK OK  : seq={ack2.payload_json()}")

    print("\nAll tests passed.")
