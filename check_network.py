"""
Network Connectivity Check
===========================
Run this on EACH machine before starting the system to verify:
  1. Your machine's IP matches what's in config.json
  2. You can reach the other two machines over UDP
  3. The required ports are not blocked by a firewall

Usage (run from project root on each machine):
    python3 check_network.py --role turbine
    python3 check_network.py --role satellite
    python3 check_network.py --role station

    # Or probe a specific IP/port directly:
    python3 check_network.py --ping 10.6.134.140 7001
"""

import sys
import os
import json
import socket
import struct
import time
import argparse

sys.path.insert(0, os.path.dirname(__file__))

# ──────────────────────────────────────────────
#  Helpers
# ──────────────────────────────────────────────
def local_ips():
    """Return all non-loopback IPv4 addresses on this machine."""
    addrs = []
    try:
        # Works on Mac/Linux
        import subprocess
        out = subprocess.check_output(
            ["ifconfig"] if sys.platform != "win32" else ["ipconfig"],
            stderr=subprocess.DEVNULL
        ).decode()
        import re
        addrs = re.findall(r'inet (?:addr:)?(\d+\.\d+\.\d+\.\d+)', out)
        addrs = [a for a in addrs if not a.startswith('127.')]
    except Exception:
        pass
    if not addrs:
        # Fallback via hostname
        try:
            hostname = socket.gethostname()
            addrs = socket.gethostbyname_ex(hostname)[2]
            addrs = [a for a in addrs if not a.startswith('127.')]
        except Exception:
            pass
    return addrs


def udp_ping(dest_ip: str, dest_port: int, timeout: float = 2.0):
    """
    Send a tiny UDP probe and listen for an echo on an ephemeral port.
    Returns (reachable, rtt_ms).
    Note: this only confirms the OTHER machine is running check_network.py
    in --listen mode (or that the port is reachable at the network layer).
    For a port-reachability check without cooperation from the other side,
    we just time the send and report 'sent' (UDP is connectionless).
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(timeout)
    probe = b'WTSP_PROBE'
    t0 = time.monotonic()
    try:
        sock.sendto(probe, (dest_ip, dest_port))
        # Try to get a response (only works if other side is listening with check_network)
        data, _ = sock.recvfrom(64)
        rtt = (time.monotonic() - t0) * 1000
        return True, rtt
    except socket.timeout:
        # No response but send succeeded — network layer reachable, app not running yet
        return None, (time.monotonic() - t0) * 1000
    except OSError as e:
        return False, 0.0
    finally:
        sock.close()


def udp_listen_echo(port: int, duration: float = 5.0):
    """Listen on a port and echo any probe back — used for cooperative ping test."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(('0.0.0.0', port))
    sock.settimeout(1.0)
    deadline = time.monotonic() + duration
    print(f"  Listening on :{port} for {duration:.0f}s …")
    while time.monotonic() < deadline:
        try:
            data, addr = sock.recvfrom(64)
            sock.sendto(b'WTSP_PONG', addr)
            print(f"  ✓ Probe from {addr[0]}:{addr[1]}")
        except socket.timeout:
            pass
    sock.close()


def check(ok: bool, msg: str):
    mark = "✓" if ok else "✗"
    print(f"  [{mark}] {msg}")
    return ok


# ──────────────────────────────────────────────
#  Role-based check
# ──────────────────────────────────────────────
def run_role_check(role: str, cfg: dict):
    my_ips = local_ips()
    print(f"\nLocal IPs detected: {', '.join(my_ips) or '(none found)'}")

    roles = {
        "turbine":   cfg["turbine"],
        "satellite": cfg["satellite"],
        "station":   cfg["station"],
    }
    my_cfg    = roles[role]
    my_ip     = my_cfg["host"]
    my_ports  = my_cfg.get("ports", {})

    print(f"\n── Role: {role.upper()} ──")
    ip_ok = check(
        my_ip in my_ips,
        f"config IP {my_ip} {'found' if my_ip in my_ips else 'NOT FOUND'} on this machine"
    )
    if not ip_ok:
        print(f"\n  ⚠  Your config says this machine is {my_ip}")
        print(f"     but local IPs are: {my_ips}")
        print(f"     → Update config.json turbine/satellite/station host to match your actual IP.")

    print(f"\n── Ports this node will bind ──")
    for name, port in my_ports.items():
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind(('0.0.0.0', port))
            check(True, f"{name} : UDP :{port} — bindable")
        except OSError as e:
            check(False, f"{name} : UDP :{port} — BIND FAILED: {e}")
        finally:
            sock.close()

    print(f"\n── Reachability to other nodes ──")
    other_roles = {r: v for r, v in roles.items() if r != role}
    for other_role, other_cfg in other_roles.items():
        other_ip    = other_cfg["host"]
        other_ports = other_cfg.get("ports", {})
        first_port  = next(iter(other_ports.values()), 7001)
        reachable, rtt = udp_ping(other_ip, first_port, timeout=1.5)
        if reachable is True:
            check(True, f"{other_role.upper()} {other_ip}:{first_port} — reachable, RTT≈{rtt:.0f}ms")
        elif reachable is None:
            # Sent OK but no echo — other app not running yet, but network layer is OK
            check(True, f"{other_role.upper()} {other_ip}:{first_port} — packet sent (no echo yet; app not running)")
        else:
            check(False, f"{other_role.upper()} {other_ip}:{first_port} — UNREACHABLE")
            print(f"         Possible causes:")
            print(f"           • '{other_ip}' is wrong — check config.json")
            print(f"           • Firewall blocking UDP on that machine (see below)")
            print(f"           • Not on the same hotspot network")

    print()
    print("── macOS Firewall reminder ──")
    print("  If reachability fails even though IPs are correct:")
    print("  System Settings → Privacy & Security → Firewall → Options")
    print("  Add python3 (or set to 'Block all incoming connections' OFF)")
    print("  Or run:  sudo /usr/libexec/ApplicationFirewall/socketfilterfw --setglobalstate off")
    print()


# ──────────────────────────────────────────────
#  Direct ping mode
# ──────────────────────────────────────────────
def run_direct_ping(ip: str, port: int):
    print(f"\nProbing {ip}:{port} …")
    reachable, rtt = udp_ping(ip, port, timeout=2.0)
    if reachable is True:
        print(f"  ✓ Reachable, RTT≈{rtt:.0f}ms")
    elif reachable is None:
        print(f"  ~ Packet sent (no echo — other side not in listen mode)")
        print(f"    Network path looks OK.")
    else:
        print(f"  ✗ Send failed (host unreachable or wrong IP)")


# ──────────────────────────────────────────────
#  Main
# ──────────────────────────────────────────────
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="WTSP network connectivity check")
    parser.add_argument('--role', choices=['turbine', 'satellite', 'station'],
                        help="Check this machine's role configuration")
    parser.add_argument('--config', default='config.json')
    parser.add_argument('--ping', nargs=2, metavar=('IP', 'PORT'),
                        help="Directly probe IP:PORT")
    parser.add_argument('--listen', type=int, metavar='PORT',
                        help="Echo-listen on PORT for cooperative ping test")
    args = parser.parse_args()

    cfg_path = os.path.join(os.path.dirname(__file__), args.config)
    with open(cfg_path) as f:
        cfg = json.load(f)

    print("=" * 54)
    print("  WTSP Network Connectivity Check")
    print("=" * 54)

    if args.listen:
        udp_listen_echo(args.listen)
    elif args.ping:
        run_direct_ping(args.ping[0], int(args.ping[1]))
    elif args.role:
        run_role_check(args.role, cfg)
    else:
        parser.print_help()
        print("\nQuick start:")
        print("  python3 check_network.py --role turbine")
        print("  python3 check_network.py --role satellite")
        print("  python3 check_network.py --role station")
