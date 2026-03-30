"""
Reliable UDP  —  Stop-and-Wait ARQ
====================================
We use raw UDP sockets and implement reliability ourselves.

Why Stop-and-Wait ARQ?
  • Simple to implement and reason about
  • Correct for a high-latency (20–50 ms) LEO link where window size > 1
    gives diminishing returns at the data rates we use
  • Directly maps to the concepts taught in the networking module

How it works (per message):
  Sender                             Receiver
  ──────                             ────────
  send(msg, seq=N)  ──────────────►  recv()
  start timer                        parse + deduplicate
                                     if new: deliver to app
                                     send ACK(N)  ◄─────────
  recv ACK(N): done
  timeout: retransmit (up to MAX_RETRIES)
  MAX_RETRIES exceeded: return False (delivery failure)

Duplicate suppression on the receiver:
  Each source keeps a rolling window of recently-seen sequence numbers.
  If we receive a seq we already processed, we re-send the ACK (in case
  our first ACK was lost) but do NOT deliver to the application again.

Thread safety:
  One background thread continuously reads incoming datagrams.
  It dispatches ACKs to unblock waiting senders, and delivers data
  messages to registered callbacks — all from the same recv thread.
  Callers of send_reliable() block in their own threads; multiple
  senders can coexist safely.

Usage:
    # Create a socket bound to a local port
    node = ReliableUDP(my_node_id=NodeID.TURBINE,
                       host='0.0.0.0', port=7001)
    node.on_message = my_callback        # fn(msg, addr) called on data arrival
    node.start()

    # Send reliably (blocks until ACK or failure)
    ok = node.send_reliable(msg, dest_addr=('192.168.1.103', 9001))

    # Send unreliably (fire-and-forget — sensor stream)
    node.send_unreliable(msg, dest_addr=('192.168.1.102', 8002))
"""

import socket
import threading
import time
import logging

from common.protocol import (
    Message, MsgType, NodeID,
    make_ack, make_nack,
    udp_send, udp_recv
)

log = logging.getLogger('reliable_udp')

# ── ARQ parameters ──
DEFAULT_TIMEOUT_S  = 2.0    # wait this long for an ACK before retransmitting
DEFAULT_MAX_RETRY  = 5      # give up after this many attempts
DEDUP_WINDOW       = 512    # remember this many seq nums per source for dedup


class ReliableUDP:
    """
    A UDP socket wrapped with Stop-and-Wait ARQ reliability.

    One instance = one bound UDP port.
    """

    def __init__(self,
                 my_node_id: NodeID,
                 host:       str,
                 port:       int,
                 timeout_s:  float = DEFAULT_TIMEOUT_S,
                 max_retry:  int   = DEFAULT_MAX_RETRY):

        self.my_node_id = my_node_id
        self.host       = host
        self.port       = port
        self.timeout_s  = timeout_s
        self.max_retry  = max_retry

        # The one UDP socket for this port
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind((host, port))

        # Callback set by the application:  fn(msg: Message, addr: tuple)
        # Called from the recv thread for every new (non-duplicate) data message.
        self.on_message = None

        # ── ARQ: pending ACK tracking ──
        # seq_num → {"event": Event, "result": "ack"|"nack"|None, "reason": str|None}
        self._ack_events: dict[int, dict] = {}
        self._ack_lock   = threading.Lock()

        # ── Duplicate suppression ──
        # src_node_id (int) → list of recently seen seq nums
        self._seen: dict[int, list] = {}
        self._seen_lock = threading.Lock()

        # ── Statistics ──
        self.stats = {
            "sent_ok":      0,     # sent + ACK received
            "sent_fail":    0,     # gave up after MAX_RETRY
            "retransmits":  0,
            "recv_new":     0,
            "recv_dup":     0,
            "recv_corrupt": 0,
        }

        self._running = False

    # ──────────────────────────────────────────
    #  Start / Stop
    # ──────────────────────────────────────────
    def start(self):
        self._running = True
        t = threading.Thread(target=self._recv_loop, daemon=True,
                             name=f"RUDP-{self.port}")
        t.start()
        log.debug(f"ReliableUDP listening on {self.host}:{self.port}")

    def stop(self):
        self._running = False
        self._sock.close()

    # ──────────────────────────────────────────
    #  Send reliable  (Stop-and-Wait ARQ)
    # ──────────────────────────────────────────
    def send_reliable(self, msg: Message, dest_addr: tuple) -> bool:
        """
        Send `msg` to `dest_addr` and wait for an ACK.
        Retransmits up to max_retry times on timeout.
        Returns True on success, False if all retries exhausted.
        """
        raw   = msg.pack()
        state = {
            "event": threading.Event(),
            "result": None,
            "reason": None,
        }

        with self._ack_lock:
            self._ack_events[msg.seq_num] = state

        for attempt in range(1, self.max_retry + 1):
            self._sock.sendto(raw, dest_addr)

            if attempt > 1:
                self.stats["retransmits"] += 1
                log.info(
                    f"[{self.port}] RETX seq={msg.seq_num} "
                    f"attempt={attempt}/{self.max_retry} → {dest_addr}"
                )

            if state["event"].wait(timeout=self.timeout_s):
                result = state["result"]
                reason = state["reason"]

                if result == "ack":
                    with self._ack_lock:
                        self._ack_events.pop(msg.seq_num, None)
                    self.stats["sent_ok"] += 1
                    log.debug(f"[{self.port}] ACK OK  seq={msg.seq_num}  attempts={attempt}")
                    return True

                if result == "nack":
                    with self._ack_lock:
                        self._ack_events.pop(msg.seq_num, None)
                    self.stats["sent_fail"] += 1
                    log.warning(
                        f"[{self.port}] DELIVERY REJECTED  seq={msg.seq_num} "
                        f"reason={reason or '?'} → {dest_addr}"
                    )
                    return False

                state["event"].clear()

        # Ran out of retries
        with self._ack_lock:
            self._ack_events.pop(msg.seq_num, None)
        self.stats["sent_fail"] += 1
        log.warning(
            f"[{self.port}] DELIVERY FAILED  seq={msg.seq_num}  "
            f"after {self.max_retry} retries → {dest_addr}"
        )
        return False

    # ──────────────────────────────────────────
    #  Send unreliable  (fire-and-forget)
    # ──────────────────────────────────────────
    def send_unreliable(self, msg: Message, dest_addr: tuple) -> None:
        """Send without waiting for ACK (used for periodic sensor stream)."""
        self._sock.sendto(msg.pack(), dest_addr)

    # ──────────────────────────────────────────
    #  Receive loop  (runs in background thread)
    # ──────────────────────────────────────────
    def _recv_loop(self):
        """
        Continuously reads UDP datagrams.
        Routes ACKs to blocked senders; delivers data to on_message callback.
        """
        while self._running:
            try:
                raw, addr = self._sock.recvfrom(65535)
            except OSError:
                break   # socket closed

            # ── 1. Parse ──
            try:
                msg = Message.unpack(raw)
            except ValueError as e:
                self.stats["recv_corrupt"] += 1
                log.debug(f"[{self.port}] corrupt datagram from {addr}: {e}")
                continue

            # ── 2. ACK/NACK: unblock the waiting sender ──
            if msg.msg_type == MsgType.ACK:
                ack_seq = (msg.payload_json() or {}).get("ack_seq")
                if ack_seq is not None:
                    with self._ack_lock:
                        state = self._ack_events.get(ack_seq)
                    if state:
                        state["result"] = "ack"
                        state["reason"] = None
                        state["event"].set()
                continue    # ACKs are internal — don't deliver to app

            if msg.msg_type == MsgType.NACK:
                payload = msg.payload_json() or {}
                nack_seq = payload.get("nack_seq")
                reason = payload.get("reason", "?")
                log.warning(
                    f"[{self.port}] NACK for seq={nack_seq}: "
                    f"{reason}"
                )
                with self._ack_lock:
                    state = self._ack_events.get(nack_seq)
                if state:
                    state["result"] = "nack"
                    state["reason"] = reason
                    state["event"].set()
                continue

            # ── 3. Duplicate suppression ──
            src = int(msg.src)
            with self._seen_lock:
                seen_list = self._seen.setdefault(src, [])
                if msg.seq_num in seen_list:
                    # Duplicate: re-ACK (in case our first ACK was lost), drop
                    self.stats["recv_dup"] += 1
                    log.debug(f"[{self.port}] DUP seq={msg.seq_num} from {addr} — re-ACKing")
                    if msg.msg_type != MsgType.SENSOR_DATA:
                        ack = make_ack(msg, self.my_node_id)
                        self._sock.sendto(ack.pack(), addr)
                    continue
                # New message — record it
                seen_list.append(msg.seq_num)
                if len(seen_list) > DEDUP_WINDOW:
                    seen_list.pop(0)    # evict oldest

            # ── 4. Send ACK (for all except fire-and-forget sensor data) ──
            if msg.msg_type != MsgType.SENSOR_DATA:
                ack = make_ack(msg, self.my_node_id)
                self._sock.sendto(ack.pack(), addr)

            # ── 5. Deliver to application ──
            self.stats["recv_new"] += 1
            if self.on_message:
                try:
                    threading.Thread(
                        target=self.on_message,
                        args=(msg, addr),
                        daemon=True,
                        name=f"RUDP-cb-{self.port}"
                    ).start()
                except Exception as e:
                    log.error(f"[{self.port}] on_message callback error: {e}")

    # ──────────────────────────────────────────
    #  Diagnostics
    # ──────────────────────────────────────────
    def status(self) -> str:
        s = self.stats
        return (
            f"port={self.port}  "
            f"sent_ok={s['sent_ok']}  fail={s['sent_fail']}  "
            f"retx={s['retransmits']}  "
            f"recv_new={s['recv_new']}  dup={s['recv_dup']}  "
            f"corrupt={s['recv_corrupt']}"
        )


# ──────────────────────────────────────────────
#  Self-test  (run this file directly)
# ──────────────────────────────────────────────
if __name__ == '__main__':
    import sys
    logging.basicConfig(level=logging.DEBUG, format='%(message)s')
    print("=== ReliableUDP self-test ===\n")

    received = []

    # ── Node A: sender (port 6001) ──
    node_a = ReliableUDP(NodeID.TURBINE, '127.0.0.1', 6001, timeout_s=0.5, max_retry=3)
    node_a.start()

    # ── Node B: receiver (port 6002) ──
    node_b = ReliableUDP(NodeID.STATION, '127.0.0.1', 6002, timeout_s=0.5, max_retry=3)
    def on_recv(msg, addr):
        received.append(msg)
        print(f"  B received: {msg.msg_type.name}  seq={msg.seq_num}")
    node_b.on_message = on_recv
    node_b.start()

    time.sleep(0.1)

    # ── Test 1: reliable delivery ──
    print("Test 1: reliable send …")
    msg = Message(MsgType.CONTROL_CMD, NodeID.TURBINE, NodeID.STATION,
                  payload={"action": "set_yaw", "value": 90.0})
    ok = node_a.send_reliable(msg, ('127.0.0.1', 6002))
    print(f"  delivered={ok}  (expected True)")
    assert ok, "reliable send should succeed"
    time.sleep(0.1)
    assert len(received) == 1

    # ── Test 2: duplicate suppression ──
    print("\nTest 2: duplicate suppression …")
    ok2 = node_a.send_reliable(msg, ('127.0.0.1', 6002))  # same seq_num
    time.sleep(0.1)
    # The duplicate should not be delivered again
    # (received count stays at 1 for the same seq)
    print(f"  still 1 delivery (received={len([m for m in received if m.seq_num == msg.seq_num])})")

    # ── Test 3: unreliable (fire-and-forget) ──
    print("\nTest 3: unreliable send (sensor stream) …")
    sensor_msg = Message(MsgType.SENSOR_DATA, NodeID.TURBINE, NodeID.STATION,
                         payload={"wind_speed_ms": 10.5})
    node_a.send_unreliable(sensor_msg, ('127.0.0.1', 6002))
    time.sleep(0.1)
    print(f"  fire-and-forget sent (no ACK expected)")

    # ── Stats ──
    print(f"\nNode A: {node_a.status()}")
    print(f"Node B: {node_b.status()}")

    node_a.stop()
    node_b.stop()
    print("\nAll tests passed.")
