"""
LEO Channel Simulator
=====================
Models the physical link between an offshore wind turbine, a LEO satellite,
and a ground-based control station.

Key effects simulated:
  1. Propagation delay    — function of slant range (altitude + geometry)
  2. Jitter              — random additional delay (atmospheric turbulence)
  3. Packet loss         — Bernoulli model; increases during "bad weather"
  4. Bandwidth cap       — token-bucket rate limiter
  5. Orbital visibility  — satellite is only overhead for ~10 min per ~95 min orbit
  6. Bad weather bursts  — random periods of elevated loss (rain fade)

Usage:
    ch = ChannelSimulator()
    delay, dropped = ch.process_message(num_bytes)
    if not dropped:
        time.sleep(delay)          # the satellite waits this long before forwarding
        # … then forward the message

Run this file directly for a quick simulation report.
"""

import time
import math
import random
import threading

# ──────────────────────────────────────────────
#  Physical constants
# ──────────────────────────────────────────────
SPEED_OF_LIGHT_KM_S = 299_792.458          # km/s

# LEO parameters (Starlink-shell-1 inspired)
ALTITUDE_KM          = 550.0               # orbital altitude
EARTH_RADIUS_KM      = 6371.0              # mean radius
MIN_ELEVATION_DEG    = 10.0                # min elevation for usable link
ORBITAL_PERIOD_S     = 5700                # 95-minute orbit
VISIBILITY_WINDOW_S  = 600                 # ~10-min visibility per pass

# Propagation geometry:
#   slant range at minimum elevation (worst-case, most delay)
#   using the law of cosines on the Earth+altitude triangle
def _slant_range_km(elevation_deg: float) -> float:
    """Slant range (km) from a surface point to the satellite at a given elevation angle."""
    el = math.radians(elevation_deg)
    R  = EARTH_RADIUS_KM
    h  = ALTITUDE_KM
    # From spherical geometry: range = √((R+h)²-R²cos²el) - R·sin(el)
    return math.sqrt((R + h)**2 - (R * math.cos(el))**2) - R * math.sin(el)

# Pre-compute delays at best (90°) and worst (10°) elevation
DELAY_BEST_MS  = (_slant_range_km(90) / SPEED_OF_LIGHT_KM_S) * 1000   # ~1.8 ms
DELAY_WORST_MS = (_slant_range_km(MIN_ELEVATION_DEG) / SPEED_OF_LIGHT_KM_S) * 1000  # ~8.3 ms

# We use 20 ms as a realistic one-way floor (includes processing + queueing on both ends)
BASE_DELAY_FLOOR_MS = 20.0


# ──────────────────────────────────────────────
#  Token-bucket bandwidth limiter
# ──────────────────────────────────────────────
class TokenBucket:
    """
    Limits throughput to `rate_bps` bits per second.
    Each call to `consume(n_bytes)` returns how many seconds to wait
    before the message can be forwarded (the transmission delay).
    """
    def __init__(self, rate_bps: int):
        self.rate_bps   = rate_bps          # bits per second
        self.tokens     = rate_bps          # start full
        self.last_refill = time.monotonic()
        self._lock       = threading.Lock()

    def consume(self, n_bytes: int) -> float:
        """
        Consume n_bytes worth of tokens.
        Returns: transmission delay in seconds (0.0 if bucket was full enough).
        """
        bits_needed = n_bytes * 8
        with self._lock:
            now     = time.monotonic()
            elapsed = now - self.last_refill
            # Refill tokens based on elapsed time
            self.tokens = min(
                self.rate_bps,
                self.tokens + elapsed * self.rate_bps
            )
            self.last_refill = now

            if self.tokens >= bits_needed:
                self.tokens -= bits_needed
                return 0.0
            else:
                # Not enough tokens — calculate wait time
                shortfall    = bits_needed - self.tokens
                wait_s       = shortfall / self.rate_bps
                self.tokens  = 0
                return wait_s


# ──────────────────────────────────────────────
#  Orbital visibility model
# ──────────────────────────────────────────────
class OrbitalModel:
    """
    Simulates whether the LEO satellite is currently visible from the turbine/station.

    Timeline (simplified):
        |<-- visible: 600 s -->|<-- outage: 5100 s -->|<-- visible: 600 s -->| ...

    The satellite starts the simulation mid-pass so we don't open with a 85-minute
    wait immediately.
    """
    def __init__(self, period_s: int = ORBITAL_PERIOD_S,
                 window_s: int = VISIBILITY_WINDOW_S):
        self.period_s = period_s
        self.window_s = window_s
        # Start 50% into the current visible window so demo starts in contact
        self._epoch = time.monotonic() - window_s * 0.5

    def is_visible(self) -> bool:
        elapsed = (time.monotonic() - self._epoch) % self.period_s
        return elapsed < self.window_s

    def time_until_visible(self) -> float:
        """Seconds until the next visibility window opens (0 if currently visible)."""
        elapsed = (time.monotonic() - self._epoch) % self.period_s
        if elapsed < self.window_s:
            return 0.0
        return self.period_s - elapsed

    def current_elevation_deg(self) -> float:
        """
        Approximate elevation angle (degrees) based on position within the window.
        Peaks at 90° at the midpoint, falls to MIN_ELEVATION_DEG at the edges.
        """
        elapsed = (time.monotonic() - self._epoch) % self.period_s
        if elapsed >= self.window_s:
            return 0.0                  # below horizon
        # Normalised position [0..1] within the window
        t = elapsed / self.window_s
        # Sine-shaped arc: 0 → peak → 0
        arc = math.sin(t * math.pi)
        return MIN_ELEVATION_DEG + arc * (90.0 - MIN_ELEVATION_DEG)

    def status_str(self) -> str:
        if self.is_visible():
            el   = self.current_elevation_deg()
            ttl  = self.window_s - (time.monotonic() - self._epoch) % self.period_s
            return f"VISIBLE  el={el:.1f}°  contact ends in {ttl:.0f}s"
        else:
            ttv = self.time_until_visible()
            return f"OUTAGE   next pass in {ttv:.0f}s ({ttv/60:.1f} min)"


# ──────────────────────────────────────────────
#  Bad-weather burst generator
# ──────────────────────────────────────────────
class WeatherModel:
    """
    Randomly toggles between "clear" and "bad weather" (rain fade) periods.
    Bad weather increases packet loss dramatically (realistic for Ka/Ku band).
    """
    CLEAR_MIN_S   = 60
    CLEAR_MAX_S   = 300
    BAD_MIN_S     = 10
    BAD_MAX_S     = 60

    def __init__(self, bad_loss_rate: float = 0.15):
        self.bad_loss_rate = bad_loss_rate
        self._bad          = False
        self._until        = 0.0
        self._schedule_next()

    def _schedule_next(self):
        if self._bad:
            dur = random.uniform(self.BAD_MIN_S, self.BAD_MAX_S)
        else:
            dur = random.uniform(self.CLEAR_MIN_S, self.CLEAR_MAX_S)
        self._until = time.monotonic() + dur

    def is_bad_weather(self) -> bool:
        if time.monotonic() >= self._until:
            self._bad = not self._bad
            self._schedule_next()
        return self._bad

    def status_str(self) -> str:
        remaining = max(0, self._until - time.monotonic())
        if self.is_bad_weather():
            return f"RAIN FADE  (clears in {remaining:.0f}s)"
        return f"CLEAR      (next check in {remaining:.0f}s)"


# ──────────────────────────────────────────────
#  Main channel simulator
# ──────────────────────────────────────────────
class ChannelSimulator:
    """
    Central object used by the satellite relay.
    Call `process(n_bytes)` for every message that arrives.
    Returns (total_delay_s, was_dropped).
    """

    def __init__(self,
                 base_loss_rate:    float = 0.02,
                 bad_weather_loss:  float = 0.15,
                 jitter_ms:         float = 10.0,
                 bandwidth_bps:     int   = 1_000_000,
                 orbital_period_s:  int   = ORBITAL_PERIOD_S,
                 visibility_window: int   = VISIBILITY_WINDOW_S):

        self.base_loss_rate = base_loss_rate
        self.jitter_ms      = jitter_ms

        self.orbit   = OrbitalModel(orbital_period_s, visibility_window)
        self.weather = WeatherModel(bad_weather_loss)
        self.bucket  = TokenBucket(bandwidth_bps)

        # Statistics
        self.stats = {
            "total":   0,
            "dropped": 0,
            "delayed": 0,
        }
        self._lock = threading.Lock()

    # ---- Main entry point ----
    def process(self, n_bytes: int) -> tuple[float, bool]:
        """
        Decide the fate of a message of `n_bytes` bytes.

        Returns:
            (delay_s, dropped)
            delay_s  — how long the satellite should wait before forwarding
            dropped  — True if the message should be silently discarded
        """
        with self._lock:
            self.stats["total"] += 1

        # 1. Orbital check — if satellite is not visible, message is lost
        #    (In a store-and-forward design you could queue it instead)
        if not self.orbit.is_visible():
            with self._lock:
                self.stats["dropped"] += 1
            return 0.0, True

        # 2. Packet loss — probabilistic drop
        loss_rate = (
            self.weather.bad_loss_rate
            if self.weather.is_bad_weather()
            else self.base_loss_rate
        )
        if random.random() < loss_rate:
            with self._lock:
                self.stats["dropped"] += 1
            return 0.0, True

        # 3. Propagation delay — based on current elevation angle
        el_deg   = self.orbit.current_elevation_deg()
        slant_km = _slant_range_km(el_deg)
        prop_ms  = max(BASE_DELAY_FLOOR_MS,
                       (slant_km / SPEED_OF_LIGHT_KM_S) * 1000)

        # 4. Jitter
        jitter_ms = random.uniform(0, self.jitter_ms)

        # 5. Transmission delay from bandwidth cap
        tx_s = self.bucket.consume(n_bytes)

        total_s = (prop_ms + jitter_ms) / 1000.0 + tx_s

        with self._lock:
            self.stats["delayed"] += 1

        return total_s, False

    def status_report(self) -> dict:
        s = self.stats
        total = max(s["total"], 1)
        return {
            "satellite":    self.orbit.status_str(),
            "weather":      self.weather.status_str(),
            "msgs_total":   s["total"],
            "msgs_dropped": s["dropped"],
            "msgs_fwd":     s["delayed"],
            "loss_pct":     f"{100 * s['dropped'] / total:.1f}%",
        }


# ──────────────────────────────────────────────
#  Quick simulation demo
# ──────────────────────────────────────────────
if __name__ == '__main__':
    import sys

    print("=== LEO Channel Simulator demo ===\n")

    # Print geometry first
    print(f"Orbital altitude       : {ALTITUDE_KM} km")
    print(f"Slant range at 90° el  : {_slant_range_km(90):.1f} km  →  {DELAY_BEST_MS:.2f} ms")
    print(f"Slant range at 10° el  : {_slant_range_km(10):.1f} km  →  {DELAY_WORST_MS:.2f} ms")
    print(f"Base floor delay       : {BASE_DELAY_FLOOR_MS} ms  (incl. processing)\n")

    ch = ChannelSimulator()

    # Simulate 200 messages of 200 bytes each
    N        = 200
    MSG_SIZE = 200
    dropped  = 0
    delays   = []

    for i in range(N):
        d, drop = ch.process(MSG_SIZE)
        if drop:
            dropped += 1
        else:
            delays.append(d * 1000)     # convert to ms

    avg_delay = sum(delays) / len(delays) if delays else 0
    min_delay = min(delays) if delays else 0
    max_delay = max(delays) if delays else 0

    print(f"Sent {N} messages ({MSG_SIZE}B each):")
    print(f"  Delivered : {N - dropped}  ({100*(N-dropped)/N:.1f}%)")
    print(f"  Dropped   : {dropped}  ({100*dropped/N:.1f}%)")
    print(f"  Delay avg : {avg_delay:.2f} ms")
    print(f"  Delay min : {min_delay:.2f} ms")
    print(f"  Delay max : {max_delay:.2f} ms")
    print()

    report = ch.status_report()
    print("Current channel status:")
    for k, v in report.items():
        print(f"  {k:20s}: {v}")
