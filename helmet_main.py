
# ── Standard library ──────────────────────────────────────────────────────────

import collections
import csv
import json
import logging
import math
import os
import queue
import sys
import time
import threading
from datetime import datetime
from typing import Optional, Tuple

# ── Third-party ───────────────────────────────────────────────────────────────

import numpy as np
import RPi.GPIO as GPIO
import serial
import smbus

# =============================================================================

# §1  ANSI COLOUR HELPERS

# Only dashboard_thread writes to stdout.

# =============================================================================

class _C:
    RST  = "\033[0m"
    BOLD = "\033[1m"
    DIM  = "\033[2m"
    RED  = "\033[31m"
    GRN  = "\033[32m"
    YLW  = "\033[33m"
    BLU  = "\033[34m"
    MGT  = "\033[35m"
    CYN  = "\033[36m"
    BRED = "\033[91m"
    BGRN = "\033[92m"
    BYLW = "\033[93m"
    BBLU = "\033[94m"
    BMGT = "\033[95m"
    BCYN = "\033[96m"
    BWHT = "\033[97m"

    @staticmethod
    def bar(val: float, hi: float, width: int = 22) -> str:
        frac   = max(0.0, min(1.0, val / hi))
        filled = int(frac * width)
        colour = _C.BGRN if frac < 0.40 else (_C.BYLW if frac < 0.72 else _C.BRED)
        return colour + "█" * filled + _C.DIM + "░" * (width - filled) + _C.RST

# =============================================================================

# §2  LOGGING  (file only — stdout owned by dashboard_thread)

# =============================================================================

logging.basicConfig(
    filename="helmet.log",
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s [%(threadName)s] %(message)s",
)
log = logging.getLogger()

# =============================================================================

# §3  CONFIGURATION

# =============================================================================

_DEFAULTS: dict = {
    # Physics — normal mode
    "impact_g":             3.15,
    "freefall_g":           0.40,
    "freefall_ms":          80,

    # Demo mode
    "demo_impact_g":        2.00,
    "demo_freefall_g":      0.85,
    "demo_freefall_ms":     15,
    "demo_hold_sec":        4.0,

    # AI
    "ai_crash_prob":        0.82,
    "ai_buf_size":          25,
    "ai_confirm_window":    2.0,
    "ai_pre_filter_g":      2.50,

    # Drowsiness
    "drowsy_pitch_deg":     25.0,
    "drowsy_nods":          2,
    "drowsy_window_sec":    4.0,
    "drowsy_recover_deg":   10.0,
    "nod_sustain_sec":      0.40,

    # Emergency
    "cancel_sec":           10,
    "cancel_extra_sec":     5,
    "call_duration":        35,
    "sms_retries":          1,

    # Hardware — original
    "buzzer_pin":           17,
    "button_pin":           18,
    "mpu_addr":             "0x69",
    "gsm_port":             "/dev/ttyS0",
    "gps_port":             "/dev/ttyUSB0",

    # Hardware — v5.1
    "touch_pin":            27,
    "strap_pin":            22,
    "menu_pin":             24,
    "touch_hold_sec":       2.0,
    "strap_off_sec":        2.0,

    # Debounce
    "gpio_debounce_ms":     20,    # stable-window debounce for menu/touch/strap

    # Files & contacts
    "contacts":             ["+919494844078"],
    "rider_file":           "rider.json",
    "ble_status_file":      "ble_status.txt",
    "telemetry_file":       "telemetry.json",
    "helmet_state_file":    "helmet_state.txt",
    "crash_log":            "crash_log.csv",
    "default_lat":          "17.3422348",
    "default_lon":          "78.3674875",

    # Watchdog
    "watchdog_sec":         8.0,
}

def _load_cfg() -> dict:
    cfg = _DEFAULTS.copy()
    if os.path.exists("config.json"):
        try:
            with open("config.json") as fh:
                cfg.update(json.load(fh))
        except Exception as exc:
            log.error(f"config.json: {exc} — using defaults")
    else:
        with open("config.json", "w") as fh:
            json.dump(_DEFAULTS, fh, indent=2)
        log.info("Wrote default config.json")
    return cfg

CFG = _load_cfg()

# Unpack constants (read-only after startup)

IMPACT_G         = float(CFG["impact_g"])
FREEFALL_G       = float(CFG["freefall_g"])
FREEFALL_MS      = int(CFG["freefall_ms"])
DEMO_IMPACT_G    = float(CFG["demo_impact_g"])
DEMO_FREEFALL_G  = float(CFG["demo_freefall_g"])
DEMO_FREEFALL_MS = int(CFG["demo_freefall_ms"])
DEMO_HOLD_SEC    = float(CFG["demo_hold_sec"])
AI_CRASH_PROB    = float(CFG["ai_crash_prob"])
AI_BUF_SIZE      = int(CFG["ai_buf_size"])
AI_CONFIRM_WIN   = float(CFG["ai_confirm_window"])
AI_PRE_FILTER    = float(CFG["ai_pre_filter_g"])
CANCEL_SEC       = int(CFG["cancel_sec"])
CANCEL_EXTRA     = int(CFG["cancel_extra_sec"])
CALL_DURATION    = int(CFG["call_duration"])
BUZZER_PIN       = int(CFG["buzzer_pin"])
BUTTON_PIN       = int(CFG["button_pin"])
TOUCH_PIN        = int(CFG["touch_pin"])
STRAP_PIN        = int(CFG["strap_pin"])
MENU_PIN         = int(CFG["menu_pin"])
TOUCH_HOLD_SEC   = float(CFG["touch_hold_sec"])
STRAP_OFF_SEC    = float(CFG["strap_off_sec"])
GPIO_DEBOUNCE_MS = int(CFG["gpio_debounce_ms"])
MPU_ADDR         = int(CFG["mpu_addr"], 16)
CONTACTS         = list(CFG["contacts"])
DROWSY_PITCH     = float(CFG["drowsy_pitch_deg"])
DROWSY_NODS      = int(CFG["drowsy_nods"])
DROWSY_WINDOW    = float(CFG["drowsy_window_sec"])
DROWSY_RECOVER   = float(CFG["drowsy_recover_deg"])
NOD_SUSTAIN_SEC  = float(CFG["nod_sustain_sec"])
HELMET_STATE_FILE = str(CFG["helmet_state_file"])

# =============================================================================

# §4  GPIO SETUP

# =============================================================================

GPIO.setmode(GPIO.BCM)
GPIO.setup(BUZZER_PIN, GPIO.OUT,  initial=GPIO.LOW)
GPIO.setup(BUTTON_PIN, GPIO.IN,   pull_up_down=GPIO.PUD_UP)
GPIO.setup(TOUCH_PIN,  GPIO.IN,   pull_up_down=GPIO.PUD_DOWN)
GPIO.setup(STRAP_PIN, GPIO.IN, pull_up_down=GPIO.PUD_UP)
GPIO.setup(MENU_PIN,   GPIO.IN,   pull_up_down=GPIO.PUD_UP)

def _beep(on_ms: int = 100, off_ms: int = 100, count: int = 1) -> None:
    for _ in range(count):
        GPIO.output(BUZZER_PIN, True)
        time.sleep(on_ms  / 1000)
        GPIO.output(BUZZER_PIN, False)
        time.sleep(off_ms / 1000)

def _debounced_read(pin: int, active_high: bool = True) -> bool:
    """
    Proper time-based debounce.
    Samples the pin over GPIO_DEBOUNCE_MS; returns the stable level.
    Returns the current reading immediately if it stays stable, else
    returns the previous last-stable reading (caller responsible for caching
    if persistence is needed — this function is stateless and safe to call
    at any rate).
    """
    interval = GPIO_DEBOUNCE_MS / 1000.0
    samples  = 4
    step     = interval / samples
    readings = []
    for _ in range(samples):
        readings.append(GPIO.input(pin))
        time.sleep(step)
    # Majority vote over the sample window
    high_count = sum(readings)
    state_high = high_count > (samples / 2)
    return state_high if active_high else not state_high

# =============================================================================

# §5  OLED  (128x64, I2C 0x3C)

# =============================================================================

_oled_lock = threading.Lock()
_OLED_OK   = False

try:
    import board, busio
    from PIL import Image, ImageDraw
    import adafruit_ssd1306
    _i2c  = busio.I2C(board.SCL, board.SDA)
    _oled = adafruit_ssd1306.SSD1306_I2C(128, 64, _i2c, addr=0x3C)
    _OLED_OK = True
except Exception as _oe:
    log.warning(f"OLED unavailable: {_oe}")

def display(l1: str = "", l2: str = "", l3: str = "", l4: str = "") -> None:
    """Thread-safe 4-line OLED write. Silent noop if OLED absent."""
    if not _OLED_OK:
        return
    try:
        with _oled_lock:
            img  = Image.new("1", (128, 64))
            draw = ImageDraw.Draw(img)
            for row, text in enumerate([l1, l2, l3, l4]):
                draw.text((0, row * 16), str(text)[:21], fill=255)
            _oled.image(img)
            _oled.show()
    except Exception as exc:
        log.error(f"OLED: {exc}")

# =============================================================================

# §6  STRICT STATE MACHINE

# =============================================================================

class State:
    NORMAL        = "NORMAL"
    CRASH_PENDING = "CRASH_PENDING"
    ALERTING      = "ALERTING"

_VALID_TRANSITIONS: dict = {
    State.NORMAL:        {State.CRASH_PENDING},
    State.CRASH_PENDING: {State.NORMAL, State.ALERTING},
    State.ALERTING:      {State.NORMAL},
}

_current_state = State.NORMAL
_state_lock    = threading.Lock()

def get_state() -> str:
    with _state_lock:
        return _current_state

def _set_state(new_state: str) -> bool:
    global _current_state
    with _state_lock:
        if new_state == _current_state:
            return False
        allowed = _VALID_TRANSITIONS.get(_current_state, set())
        if new_state not in allowed:
            log.warning(f"FSM: illegal {_current_state} -> {new_state} ignored")
            return False
        _current_state = new_state
        log.info(f"FSM: -> {new_state}")
    # Lock fully released — safe to call _eval_ignition() without deadlock
    _eval_ignition()
    return True

# =============================================================================

# §7  SHARED STATE CONTAINERS

# All mutable shared state lives here; accessor functions enforce thread safety.

# =============================================================================

# ── GPS ───────────────────────────────────────────────────────────────────────

_gps_lock = threading.Lock()
_gps: dict = {
    "lat":   CFG["default_lat"],
    "lon":   CFG["default_lon"],
    "speed": 0.0,
    "fixed": False,
}

# ── Rider profile ─────────────────────────────────────────────────────────────

_rider_lock = threading.Lock()
_rider: dict = {
    "name": "Unknown", "blood": "-", "contact": "-",
    "allergies": "-", "medical": "-", "belongings": "-",
}

# ── IMU scalars (GIL-safe single writer: imu_thread) ─────────────────────────

_g_now     : float = 1.0
_ax_now    : float = 0.0
_ay_now    : float = 0.0
_az_now    : float = 1.0
_pitch_now : float = 0.0

# ── Demo mode (GIL-safe single writer: demo_watch_thread) ────────────────────

_demo_mode : bool = False

# ── Helmet wear state ─────────────────────────────────────────────────────────

_helmet_lock   = threading.Lock()
_helmet_worn   = False
_touch_active  = False   # last debounced touch reading (informational)
_strap_latched = False   # last debounced strap reading (informational)

def get_helmet_worn() -> bool:
    with _helmet_lock:
        return _helmet_worn

def _set_helmet_worn(value: bool) -> None:
    global _helmet_worn
    with _helmet_lock:
        _helmet_worn = value

# ── Ignition state ────────────────────────────────────────────────────────────

_ignition_lock = threading.Lock()
_ignition_on   = False

def get_ignition() -> bool:
    with _ignition_lock:
        return _ignition_on

def _set_ignition(value: bool) -> None:
    global _ignition_on
    with _ignition_lock:
        _ignition_on = value

# ── OLED display mode ─────────────────────────────────────────────────────────

_OLED_MODES      = ["MAIN", "HELMET", "SENSORS", "GPS", "SYSTEM"]
_oled_mode_lock  = threading.Lock()
_oled_mode_index = 0

def get_oled_mode() -> str:
    with _oled_mode_lock:
        return _OLED_MODES[_oled_mode_index]

def _cycle_oled_mode() -> str:
    global _oled_mode_index
    with _oled_mode_lock:
        _oled_mode_index = (_oled_mode_index + 1) % len(_OLED_MODES)
        mode = _OLED_MODES[_oled_mode_index]
        log.info(f"OLED mode: {mode}")
        return mode

# ── Cross-thread events ───────────────────────────────────────────────────────

crash_event  = threading.Event()
drowsy_event = threading.Event()

# ── AI confirmation ───────────────────────────────────────────────────────────

_ai_lock       = threading.Lock()
_ai_confirmed  = False
_ai_prob_last  = 0.0

def _reset_ai_confirm() -> None:
    global _ai_confirmed, _ai_prob_last
    with _ai_lock:
        _ai_confirmed = False
        _ai_prob_last = 0.0

def _set_ai_confirmed(prob: float) -> None:
    global _ai_confirmed, _ai_prob_last
    with _ai_lock:
        _ai_confirmed = True
        _ai_prob_last = prob

def get_ai_confirmed() -> bool:
    with _ai_lock:
        return _ai_confirmed

def get_ai_prob() -> float:
    with _ai_lock:
        return _ai_prob_last

# =============================================================================

# §8  TELEMETRY FILE

# =============================================================================

_tele_lock  = threading.Lock()
_tele_cache: dict = {
    "g":        1.0,
    "speed":    0.0,
    "lat":      CFG["default_lat"],
    "lon":      CFG["default_lon"],
    "gps_fix":  0,
    "crash":    0,
    "battery":  85,
    "demo":     0,
    "drowsy":   0,
    "helmet":   0,
    "ignition": 0,
}

def _tele_update(**kwargs) -> None:
    with _tele_lock:
        _tele_cache.update(kwargs)

def _tele_flush() -> None:
    """Atomic write via tmp + os.replace."""
    tmp = CFG["telemetry_file"] + ".tmp"
    try:
        with _tele_lock:
            snapshot = dict(_tele_cache)
        with open(tmp, "w") as fh:
            json.dump(snapshot, fh)
        os.replace(tmp, CFG["telemetry_file"])
    except Exception as exc:
        log.error(f"Telemetry flush: {exc}")

def _write_helmet_state(worn: bool) -> None:
    """Atomic write 1/0 to helmet_state.txt for ESP32 BLE characteristic."""
    tmp = HELMET_STATE_FILE + ".tmp"
    try:
        with open(tmp, "w") as fh:
            fh.write("1" if worn else "0")
        os.replace(tmp, HELMET_STATE_FILE)
    except Exception as exc:
        log.error(f"helmet_state write: {exc}")

# =============================================================================

# §9  DASHBOARD EVENT RING

# =============================================================================

_dash_ring  = collections.deque(maxlen=6)
_dash_lock  = threading.Lock()
_DASH_LINES = 22

def _dash_log(msg: str) -> None:
    ts = datetime.now().strftime("%H:%M:%S")
    with _dash_lock:
        _dash_ring.append(f"{_C.DIM}{ts}{_C.RST}  {msg}")

# =============================================================================

# §10  WATCHDOG HEARTBEAT

# =============================================================================

_hb: dict         = {}
_hb_lock          = threading.Lock()

def _pulse(name: str) -> None:
    with _hb_lock:
        _hb[name] = time.monotonic()

# =============================================================================

# §11  IMU — MPU6050

# =============================================================================

_bus = smbus.SMBus(1)

def _init_mpu() -> None:
    _bus.write_byte_data(MPU_ADDR, 0x6B, 0x00)
    _bus.write_byte_data(MPU_ADDR, 0x1C, 0x00)
    _bus.write_byte_data(MPU_ADDR, 0x1A, 0x03)
    log.info("MPU6050 ready (±2 g, DLPF 44 Hz)")

def _read_word(reg: int) -> int:
    h = _bus.read_byte_data(MPU_ADDR, reg)
    l = _bus.read_byte_data(MPU_ADDR, reg + 1)
    v = (h << 8) | l
    return v - 65536 if v >= 0x8000 else v

def _read_accel() -> Tuple[float, float, float, float, float]:
    ax = _read_word(0x3B) / 16384.0
    ay = _read_word(0x3D) / 16384.0
    az = _read_word(0x3F) / 16384.0
    g  = math.sqrt(ax * ax + ay * ay + az * az)
    pitch = math.degrees(math.asin(max(-1.0, min(1.0, ay / g)))) if g > 0.1 else 0.0
    return ax, ay, az, g, pitch

# =============================================================================

# §12  PHYSICS CRASH DETECTOR

# =============================================================================

_G_SMOOTH_LEN  = 3
_g_smooth_buf  = collections.deque(maxlen=_G_SMOOTH_LEN)

def _smooth_g(raw: float) -> float:
    _g_smooth_buf.append(raw)
    return sum(_g_smooth_buf) / len(_g_smooth_buf)

class _PhysicsFSM:
    """
    Freefall → impact FSM.
    Public API:
        feed(g) -> bool        — returns True on crash detection
        take_peak() -> float   — returns and resets peak g accumulator
    """
    __slots__ = ("_in_ff", "_ff_start", "_peak", "_last_trigger")

    def __init__(self) -> None:
        self._in_ff        = False
        self._ff_start     = 0.0
        self._peak         = 0.0
        self._last_trigger = 0.0

    def feed(self, g: float) -> bool:
        if g > self._peak:
            self._peak = g
        sg  = _smooth_g(g)
        now = time.monotonic()

        if _demo_mode:
            if (now - self._last_trigger) < 2.0:
                return False
            if sg >= DEMO_IMPACT_G:
                self._last_trigger = now
                log.warning(f"Physics crash [DEMO]: {sg:.2f} g / {g:.2f} g raw")
                return True
            return False

        full_cooldown = float(CANCEL_SEC + CANCEL_EXTRA + 2)
        if (now - self._last_trigger) < full_cooldown:
            return False

        if not self._in_ff:
            if sg < FREEFALL_G:
                self._in_ff    = True
                self._ff_start = now
        else:
            elapsed_ms = (now - self._ff_start) * 1000.0
            if sg > IMPACT_G and elapsed_ms >= FREEFALL_MS:
                self._in_ff        = False
                self._last_trigger = now
                log.warning(
                    f"Physics crash [LIVE]: {sg:.2f} g / {g:.2f} g raw "
                    f"after {elapsed_ms:.0f} ms freefall"
                )
                return True
            if sg >= FREEFALL_G:
                self._in_ff = False
        return False

    def take_peak(self) -> float:
        """Return accumulated peak g and reset accumulator."""
        p, self._peak = self._peak, 0.0
        return p

_physics_fsm = _PhysicsFSM()

# =============================================================================

# §13  AI / TINYML DETECTOR  (numpy MLP — confirmation layer only)

# =============================================================================

class _TinyMLP:
    _W1 = np.array([
        [ 4.0,  2.5,  1.0,  1.5,  1.5,  3.0,  2.0,  2.5],
        [ 0.5,  3.5,  0.5,  0.0,  0.5,  0.0,  3.0,  2.0],
        [ 1.0,  0.5,  4.0,  2.0,  2.0,  0.5,  1.0,  1.5],
        [ 1.5,  1.5,  3.0,  2.5,  1.5,  1.0,  2.5,  2.0],
        [ 2.0,  1.0,  0.5,  3.0,  1.0,  3.5,  0.5,  1.5],
        [ 0.0,  1.5,  0.5,  0.0,  0.5,  0.0,  2.5,  1.0],
        [ 0.5,  1.0,  1.0,  4.0,  0.5,  0.5,  1.5,  1.5],
        [ 0.0,  3.0,  0.5, -0.5,  0.0, -0.5,  3.0,  1.5],
        [ 3.5,  2.0,  1.0,  1.5,  2.5,  2.5,  2.0,  2.5],
        [ 1.0,  0.5,  2.0,  1.0,  4.0,  2.0,  0.5,  1.5],
    ], dtype=np.float32)
    _b1 = np.array([-3.5, -3.5, -2.5, -2.0, -2.5, -2.5, -4.0, -4.5], dtype=np.float32)
    _W2 = np.array([[2.0], [2.5], [1.5], [1.5], [1.5], [1.5], [2.5], [2.0]], dtype=np.float32)
    _b2 = np.array([-5.5], dtype=np.float32)

    @staticmethod
    def _sig(x: np.ndarray) -> np.ndarray:
        return 1.0 / (1.0 + np.exp(-np.clip(x, -30.0, 30.0)))

    def _features(self, buf: list) -> np.ndarray:
        g_arr  = np.array([s[3] for s in buf], dtype=np.float32)
        max_g  = float(g_arr.max())
        min_g  = float(g_arr.min())
        std_g  = float(g_arr.std())
        recent = float(g_arr[-5:].mean())
        early  = float(g_arr[:5].mean())
        delta  = max(recent - early, 0.0)
        n_ff   = float((g_arr < FREEFALL_G).mean())
        n_imp  = float((g_arr > IMPACT_G).mean())
        rms_g  = float(np.sqrt((g_arr ** 2).mean()))
        raw = np.array([
            max_g / 6.0,
            1.0 - min_g,
            std_g / 2.0,
            (max_g - min_g) / 6.0,
            recent / 5.0,
            1.0 - min(early / 2.0, 1.0),
            delta / 4.0,
            n_ff,
            n_imp,
            rms_g / 5.0,
        ], dtype=np.float32)
        return np.clip(raw, 0.0, 1.0)

    def infer(self, buf: list) -> float:
        peak = max(s[3] for s in buf)
        if peak < AI_PRE_FILTER:
            return 0.0
        f = self._features(buf)
        h = self._sig(f @ self._W1 + self._b1)
        o = self._sig(h @ self._W2 + self._b2)
        return float(o[0])

_mlp = _TinyMLP()

_ai_buf      = collections.deque(maxlen=AI_BUF_SIZE)
_ai_buf_lock = threading.Lock()

# =============================================================================

# §14  DROWSINESS DETECTOR

# =============================================================================

class _DrowsyFSM:
    __slots__ = ("_nodding", "_nod_enter_time", "_nod_times", "_alert_since")

    def __init__(self) -> None:
        self._nodding        = False
        self._nod_enter_time = 0.0
        self._nod_times      = collections.deque()
        self._alert_since    = 0.0

    def feed(self, pitch: float) -> None:
        now = time.monotonic()
        if not self._nodding:
            if pitch > DROWSY_PITCH:
                self._nodding        = True
                self._nod_enter_time = now
        else:
            if pitch < DROWSY_RECOVER:
                dip_secs      = now - self._nod_enter_time
                self._nodding = False
                if dip_secs >= NOD_SUSTAIN_SEC:
                    self._nod_times.append(now)
                    log.debug(f"Nod counted — dip {dip_secs:.2f}s")
                else:
                    log.debug(f"Nod ignored — dip {dip_secs:.3f}s < {NOD_SUSTAIN_SEC}s")

        while self._nod_times and (now - self._nod_times[0]) > DROWSY_WINDOW:
            self._nod_times.popleft()

        if len(self._nod_times) >= DROWSY_NODS and not drowsy_event.is_set():
            log.warning(f"Drowsiness: {len(self._nod_times)} nods in {DROWSY_WINDOW:.0f}s")
            drowsy_event.set()
            self._alert_since = now
            self._nod_times.clear()

        if drowsy_event.is_set() and pitch < DROWSY_RECOVER:
            if (now - self._alert_since) > 3.0:
                drowsy_event.clear()
                log.info("Drowsiness alert cleared")

_drowsy_fsm = _DrowsyFSM()

# =============================================================================

# §15  HELMET WEAR FSM  (v5.1)

# =============================================================================

class _HelmetFSM:
    """
    Finite state machine for helmet wear detection.
    Single writer: helmet_thread. All reads via get_helmet_worn().

    WORN detection:
        Touch must be held >= TOUCH_HOLD_SEC AND strap must be latched.

    REMOVAL detection:
        Strap must be absent for >= STRAP_OFF_SEC.
    """
    __slots__ = ("_touch_start", "_strap_off_start")

    def __init__(self) -> None:
        self._touch_start     = 0.0
        self._strap_off_start = 0.0

    def feed(self, touch: bool, strap: bool) -> bool:
        """
        Call at 20 Hz. Returns True if helmet worn-state changed this cycle.
        touch — True if capacitive sensor active
        strap — True if strap latched
        """
        global _touch_active, _strap_latched
        _touch_active  = touch
        _strap_latched = strap

        now     = time.monotonic()
        worn    = get_helmet_worn()
        changed = False

        if not worn:
            # ── Wait to confirm WORN ──────────────────────────────────────
            if touch:
                if self._touch_start == 0.0:
                    self._touch_start = now
                elif (now - self._touch_start) >= TOUCH_HOLD_SEC:
                    if strap:
                        self._touch_start     = 0.0
                        self._strap_off_start = 0.0
                        _set_helmet_worn(True)
                        _write_helmet_state(True)
                        _tele_update(helmet=1)
                        log.info("Helmet WORN confirmed (touch held + strap latched)")
                        _dash_log(f"{_C.BGRN}Helmet WORN{_C.RST}")
                        changed = True
            else:
                self._touch_start = 0.0

        else:
            # ── Monitor for REMOVAL ───────────────────────────────────────
            if not strap:
                if self._strap_off_start == 0.0:
                    self._strap_off_start = now
                elif (now - self._strap_off_start) >= STRAP_OFF_SEC:
                    self._strap_off_start = 0.0
                    self._touch_start     = 0.0
                    _set_helmet_worn(False)
                    _write_helmet_state(False)
                    _tele_update(helmet=0)
                    log.info("Helmet REMOVED confirmed (strap off)")
                    _dash_log(f"{_C.BYLW}Helmet REMOVED{_C.RST}")
                    changed = True
            else:
                self._strap_off_start = 0.0

        return changed

_helmet_fsm = _HelmetFSM()

# =============================================================================

# §16  IGNITION LOGIC

# =============================================================================

def _eval_ignition() -> None:
    """
    Recompute and apply ignition state. Safe to call from any thread.
    ON iff helmet WORN AND state NORMAL.
    FORCE OFF during CRASH_PENDING / ALERTING.
    """
    worn      = get_helmet_worn()
    state     = get_state()
    should_on = worn and (state == State.NORMAL)
    prev      = get_ignition()
    if should_on != prev:
        _set_ignition(should_on)
        _tele_update(ignition=1 if should_on else 0)
        tag = (f"{_C.BGRN}IGNITION ON{_C.RST}" if should_on
               else f"{_C.BRED}IGNITION OFF{_C.RST}")
        log.info(f"Ignition {'ON' if should_on else 'OFF'} "
                 f"(worn={worn} state={state})")
        _dash_log(tag)

# =============================================================================

# §17  THREAD REGISTRY  (supports safe watchdog restarts)

# =============================================================================

# Registry: name -> (target_fn, thread, stop_event)

_thread_registry: dict  = {}
_thread_reg_lock        = threading.Lock()

def _spawn(name: str, stop_event: threading.Event) -> threading.Thread:
    """Create and start a daemon thread for the named function."""
    fn = _thread_registry[name][0]

    def _wrapper():
        try:
            fn(stop_event)
        except Exception as exc:
            log.error(f"Thread '{name}' crashed: {exc}", exc_info=True)

    t = threading.Thread(target=_wrapper, name=name, daemon=True)
    t.start()
    log.info(f"Thread '{name}' started (ident={t.ident})")
    return t

def _register_and_start(name: str, fn) -> None:
    """Register a thread function and start it immediately."""
    stop_event = threading.Event()
    with _thread_reg_lock:
        _thread_registry[name] = (fn, None, stop_event)
    t = _spawn(name, stop_event)
    with _thread_reg_lock:
        _thread_registry[name] = (fn, t, stop_event)

def _restart_thread(name: str) -> None:
    """
    Safely stop the old thread instance and spawn a fresh one.
    Called only from watchdog_thread.
    """
    with _thread_reg_lock:
        entry = _thread_registry.get(name)
        if entry is None:
            return

        fn, old_thread, old_stop = entry

    # Signal old thread to stop
    old_stop.set()
    if old_thread is not None and old_thread.is_alive():
        old_thread.join(timeout=3.0)
        if old_thread.is_alive():
            log.warning(f"Thread '{name}' did not stop cleanly within 3s")

    # Fresh stop event and thread
    new_stop = threading.Event()
    new_thread = _spawn(name, new_stop)
    with _thread_reg_lock:
        _thread_registry[name] = (fn, new_thread, new_stop)

    log.info(f"Thread '{name}' restarted (ident={new_thread.ident})")

# =============================================================================

# §18  THREAD FUNCTIONS

# All thread functions accept a single threading.Event (stop_event) argument.

# They must check stop_event.is_set() regularly and exit when set.

# =============================================================================

# ── §18.1  IMU THREAD (50 Hz) ─────────────────────────────────────────────────

def _imu_thread(stop: threading.Event) -> None:
    global _g_now, _ax_now, _ay_now, _az_now, _pitch_now
    try:
        os.nice(-10)
    except Exception:
        pass

    while not stop.is_set():
        try:
            ax, ay, az, g, pitch = _read_accel()
            _g_now     = g
            _ax_now    = ax
            _ay_now    = ay
            _az_now    = az
            _pitch_now = pitch

            if _physics_fsm.feed(g) and not crash_event.is_set():
                _reset_ai_confirm()
                crash_event.set()
                tag = "[DEMO]" if _demo_mode else "[LIVE]"
                _dash_log(f"{_C.BRED}Physics crash {tag}  {g:.2f} g{_C.RST}")

            with _ai_buf_lock:
                _ai_buf.append((ax, ay, az, g))

            _tele_update(
                g      = round(g, 3),
                crash  = 1 if crash_event.is_set() else 0,
                demo   = 1 if _demo_mode else 0,
                drowsy = 1 if drowsy_event.is_set() else 0,
            )

        except Exception as exc:
            log.error(f"IMU: {exc}")

        _pulse("imu")
        time.sleep(0.02)

# ── §18.2  AI THREAD (5 Hz) ───────────────────────────────────────────────────

def _ai_thread(stop: threading.Event) -> None:
    cooldown_until = 0.0

    while not stop.is_set():
        try:
            now = time.monotonic()
            if now >= cooldown_until:
                with _ai_buf_lock:
                    buf_ready = len(_ai_buf) >= AI_BUF_SIZE
                    snap = list(_ai_buf) if buf_ready else []

                if buf_ready:
                    prob = _mlp.infer(snap)

                    if crash_event.is_set() and prob >= AI_CRASH_PROB:
                        _set_ai_confirmed(prob)
                        log.info(f"AI confirmed crash  prob={prob:.3f}")
                        _dash_log(f"{_C.BMGT}AI confirmed  prob={prob:.2f}{_C.RST}")
                        cooldown_until = now + 3.0
                    elif prob > 0.60 and not crash_event.is_set():
                        log.info(f"AI elevated (no physics trigger): prob={prob:.3f}")

        except Exception as exc:
            log.error(f"AI thread: {exc}")

        _pulse("ai")
        time.sleep(0.2)

# ── §18.3  DROWSY THREAD (10 Hz) ─────────────────────────────────────────────

def _drowsy_thread(stop: threading.Event) -> None:
    buzzing = False

    while not stop.is_set():
        try:
            pitch = _pitch_now
            state = get_state()
            worn = get_helmet_worn()

            if state == State.NORMAL and worn:
                _drowsy_fsm.feed(pitch)
            else:
                if drowsy_event.is_set():
                    drowsy_event.clear()
                _drowsy_fsm._nod_times.clear()
                _drowsy_fsm._nodding = False

            alert = drowsy_event.is_set() and not crash_event.is_set()

            if alert:
                if not buzzing:
                    buzzing = True
                    log.info("Drowsy buzzer ON")
                    _dash_log(f"{_C.BMGT}DROWSINESS — buzzer active{_C.RST}")
                    display("!! DROWSINESS !!", "Head Nod Detected", "Stay Alert!", "")
                GPIO.output(BUZZER_PIN, True)
                time.sleep(0.20)
                GPIO.output(BUZZER_PIN, False)
                time.sleep(0.30)
            else:
                if buzzing:
                    buzzing = False
                    GPIO.output(BUZZER_PIN, False)
                    _dash_log(f"{_C.BGRN}Drowsiness cleared{_C.RST}")
                time.sleep(0.10)

        except Exception as exc:
            log.error(f"Drowsy thread: {exc}")
            time.sleep(0.5)

        _pulse("drowsy")

# ── §18.4  DEMO WATCH THREAD (20 Hz) ─────────────────────────────────────────

def _demo_watch_thread(stop: threading.Event) -> None:
    global _demo_mode
    held_since  = 0.0
    is_holding  = False
    triple_buf  = collections.deque(maxlen=3)
    last_toggle = 0.0
    POLL        = 0.05

    while not stop.is_set():
        try:
            if crash_event.is_set():
                is_holding = False
                _pulse("demo_watch")
                time.sleep(POLL)
                continue

            pressed = (GPIO.input(BUTTON_PIN) == GPIO.LOW)
            now     = time.monotonic()

            if pressed:
                if not is_holding:
                    is_holding = True
                    held_since = now
                elif (now - held_since) >= DEMO_HOLD_SEC and (now - last_toggle) > 1.0:
                    _demo_mode  = not _demo_mode
                    last_toggle = now
                    is_holding  = False
                    _announce_demo()
                    time.sleep(0.6)
            else:
                if is_holding:
                    held = now - held_since
                    if held < DEMO_HOLD_SEC:
                        triple_buf.append(now)
                        if (len(triple_buf) == 3
                                and (now - triple_buf[0]) <= 2.0
                                and (now - last_toggle) > 1.0):
                            _demo_mode  = not _demo_mode
                            last_toggle = now
                            triple_buf.clear()
                            _announce_demo()
                    is_holding = False

        except Exception as exc:
            log.error(f"Demo watch: {exc}")

        _pulse("demo_watch")
        time.sleep(POLL)

def _announce_demo() -> None:
    state = "ENABLED" if _demo_mode else "DISABLED"
    log.info(f"Demo mode {state}")
    _dash_log(f"{_C.BYLW}Demo mode {state}{_C.RST}")
    display("** DEMO MODE **", state, "5s hold=toggle", "")
    _beep(80, 120, 2)

# ── §18.5  GPS THREAD (event-driven) ─────────────────────────────────────────

def _gps_thread(stop: threading.Event) -> None:
    while not stop.is_set():
        try:
            ser = serial.Serial(CFG["gps_port"], 9600, timeout=1)
            log.info("GPS serial open")
            while not stop.is_set():
                try:
                    line = ser.readline().decode(errors="ignore")
                except Exception:
                    break
                if "$GPRMC" not in line:
                    continue
                try:
                    p = line.split(",")
                    if p[2] != "A":
                        raise ValueError("no fix")
                    lat = float(p[3]); lon = float(p[5])
                    lat = int(lat / 100) + (lat % 100) / 60.0
                    lon = int(lon / 100) + (lon % 100) / 60.0
                    spd = float(p[7]) * 1.852 if p[7] else 0.0
                    with _gps_lock:
                        _gps["lat"]   = str(round(lat, 6))
                        _gps["lon"]   = str(round(lon, 6))
                        _gps["speed"] = round(spd, 1)
                        _gps["fixed"] = True
                    _tele_update(
                        lat=_gps["lat"], lon=_gps["lon"],
                        speed=round(spd, 1), gps_fix=1,
                    )
                except Exception:
                    pass
            _pulse("gps")
            ser.close()
        except Exception as exc:
            log.error(f"GPS serial: {exc}")
        if not stop.is_set():
            time.sleep(5)

# ── §18.6  RIDER THREAD (3 s poll) ───────────────────────────────────────────

def _rider_thread(stop: threading.Event) -> None:
    while not stop.is_set():
        try:
            with open(CFG["rider_file"]) as fh:
                data = json.load(fh)
            with _rider_lock:
                _rider.update(data)
        except Exception:
            pass
        _pulse("rider")
        stop.wait(3.0)

# ── §18.7  HELMET THREAD (20 Hz) ─────────────────────────────────────────────

def _helmet_thread(stop: threading.Event) -> None:
    """
    Reads touch and strap pins with proper time-based debouncing.
    Calls _eval_ignition() only when helmet state actually changes.
    """
    DEBOUNCE_SAMPLES = max(2, GPIO_DEBOUNCE_MS // 5)
    SAMPLE_INTERVAL  = 0.005  # 5 ms between samples

    def _stable_read(pin: int) -> bool:
        """Majority vote over DEBOUNCE_SAMPLES readings spaced SAMPLE_INTERVAL apart."""
        highs = 0
        for _ in range(DEBOUNCE_SAMPLES):
            if GPIO.input(pin) == GPIO.HIGH:
                highs += 1
            time.sleep(SAMPLE_INTERVAL)
        return highs > (DEBOUNCE_SAMPLES / 2)

    while not stop.is_set():
        try:
            touch = _stable_read(TOUCH_PIN)
            strap = not _stable_read(STRAP_PIN)

            changed = _helmet_fsm.feed(touch, strap)
            if changed:
                # Immediately re-evaluate ignition on state change
                _eval_ignition()

        except Exception as exc:
            log.error(f"Helmet thread: {exc}")

        _pulse("helmet")
        # Sampling already takes DEBOUNCE_SAMPLES * SAMPLE_INTERVAL ≈ 20 ms
        # Add a small gap to achieve ~20 Hz net rate
        time.sleep(0.03)

# ── §18.8  MENU THREAD (20 Hz) ───────────────────────────────────────────────

def _menu_thread(stop: threading.Event) -> None:
    """
    Proper falling-edge detection with time-based debounce.
    No double-read hack: uses a stable-window approach.
    """
    DEBOUNCE_S   = 0.20
    SAMPLE_STEP  = 0.005
    STABLE_COUNT = max(2, int(DEBOUNCE_S / SAMPLE_STEP / 10))  # small window

    prev_stable  = False  # previous debounced state (not-pressed = False)
    last_press   = 0.0

    def _read_menu_pressed() -> bool:
        """Return True if MENU_PIN is stably LOW (active-low)."""
        low_count = 0
        checks    = 4
        for _ in range(checks):
            if GPIO.input(MENU_PIN) == GPIO.LOW:
                low_count += 1
            time.sleep(SAMPLE_STEP)
        return low_count > (checks / 2)

    while not stop.is_set():
        try:
            now     = time.monotonic()
            pressed = _read_menu_pressed()

            # Falling edge: not-pressed → pressed, with debounce cooldown
            if pressed and not prev_stable:
                if (now - last_press) >= DEBOUNCE_S:
                    last_press = now
                    mode = _cycle_oled_mode()
                    _beep(30, 0, 1)
                    _dash_log(f"{_C.BCYN}Display mode: {mode}{_C.RST}")

            prev_stable = pressed

        except Exception as exc:
            log.error(f"Menu thread: {exc}")

        _pulse("menu")
        time.sleep(0.05)

# ── §18.9  OLED THREAD (0.5 s, redraw only on change) ────────────────────────

def _oled_thread(stop: threading.Event) -> None:
    """
    Computes a content tuple each cycle.
    Calls display() ONLY when the tuple differs from the previous render,
    or when a crash/drowsy priority override is active.
    """
    _prev_content: tuple = ()

    while not stop.is_set():
        try:
            mode  = get_oled_mode()
            state = get_state()
            demo  = _demo_mode
            dry   = drowsy_event.is_set()
            worn  = get_helmet_worn()
            ign   = get_ignition()
            g     = _g_now
            pitch = _pitch_now
            ax, ay, az = _ax_now, _ay_now, _az_now
            crash = crash_event.is_set()

            with _gps_lock:
                spd   = _gps["speed"]
                lat   = _gps["lat"]
                lon   = _gps["lon"]
                fixed = _gps["fixed"]
            with _rider_lock:
                name = _rider.get("name", "Unknown")[:12]
            ai_p = get_ai_prob()

            # Build content tuple for the current mode
            if mode == "MAIN":
                content = (
                    f"G:{g:.2f}g {state[:6]}",
                    f"Hmt:{'WORN' if worn else 'OFF'} IGN:{'ON' if ign else 'OFF'}",
                    f"CRS:{'YES' if crash else 'NO '} {spd:.0f}km GPS:{'Y' if fixed else 'N'}",
                    datetime.now().strftime("%H:%M:%S"),
                )
            elif mode == "HELMET":
                touch_str = "HI" if _touch_active else "LO"
                strap_str = "ON" if _strap_latched else "OFF"
                content = (
                    "-- HELMET --",
                    f"State: {'WORN' if worn else 'NOT WORN'}",
                    f"Strap:{strap_str}  Touch:{touch_str}",
                    f"Ignition: {'ON' if ign else 'OFF'}",
                )
            elif mode == "SENSORS":
                content = (
                    "-- SENSORS --",
                    f"G:{g:.3f}g  P:{pitch:+.1f}",
                    f"aX:{ax:+.2f} aY:{ay:+.2f}",
                    f"aZ:{az:+.2f}",
                )
            elif mode == "GPS":
                content = (
                    "-- GPS --",
                    f"{'FIXED' if fixed else 'NO FIX'}  {spd:.1f}km/h",
                    f"La:{lat[:9]}",
                    f"Lo:{lon[:9]}",
                )
            elif mode == "SYSTEM":
                tag = "[DEMO]" if demo else ("[DRSY]" if dry else "OK")
                content = (
                    "-- SYSTEM --",
                    f"AI:{ai_p:.2f}  {tag}",
                    f"State:{state[:8]}",
                    name,
                )
            else:
                content = ("", "", "", "")

            # Priority overrides (always redraw)
            if crash:
                content = (
                    "!! CRASH DETECTED !!",
                    f"AI:{'YES' if ai_p >= AI_CRASH_PROB else 'NO '}",
                    f"State:{state[:8]}",
                    datetime.now().strftime("%H:%M:%S"),
                )
            elif dry:
                content = ("!! DROWSINESS !!", "Head Nod Detected", "Stay Alert!", "")

            # Only write to OLED when content changed
            if content != _prev_content:
                display(*content)
                _prev_content = content

        except Exception as exc:
            log.error(f"OLED thread: {exc}")

        _pulse("oled")
        time.sleep(0.5)

# ── §18.10  TELEMETRY THREAD (1 s) ────────────────────────────────────────────

def _telemetry_thread(stop: threading.Event) -> None:
    while not stop.is_set():
        try:
            with _gps_lock:
                spd = _gps["speed"]
            _tele_update(
                speed    = round(spd, 1),
                helmet   = 1 if get_helmet_worn() else 0,
                ignition = 1 if get_ignition() else 0,
            )
            _tele_flush()
        except Exception as exc:
            log.error(f"Telemetry thread: {exc}")
        _pulse("telemetry")
        stop.wait(1.0)

# ── §18.11  DASHBOARD THREAD (0.5 s) ─────────────────────────────────────────

def _dashboard_thread(stop: threading.Event) -> None:
    sys.stdout.write("\n" * _DASH_LINES)
    sys.stdout.flush()
    while not stop.is_set():
        try:
            _redraw_dashboard()
        except Exception as exc:
            log.error(f"Dashboard: {exc}")
        _pulse("dashboard")
        time.sleep(0.5)

def _redraw_dashboard() -> None:
    g     = _g_now
    pitch = _pitch_now
    state = get_state()
    demo  = _demo_mode
    dry   = drowsy_event.is_set()
    ai_p  = get_ai_prob()
    worn  = get_helmet_worn()
    ign   = get_ignition()
    mode  = get_oled_mode()
    touch = _touch_active
    strap = _strap_latched

    with _gps_lock:
        lat  = _gps["lat"]
        lon  = _gps["lon"]
        spd  = _gps["speed"]
        fix  = _gps["fixed"]
    with _rider_lock:
        name  = _rider.get("name",  "Unknown")[:16]
        blood = _rider.get("blood", "-")

    state_col = {
        State.NORMAL:        _C.BGRN,
        State.CRASH_PENDING: _C.BRED,
        State.ALERTING:      _C.BRED + _C.BOLD,
    }.get(state, _C.BWHT)

    g_col   = _C.BGRN if g < 2.0 else (_C.BYLW if g < IMPACT_G else _C.BRED)
    thr_now = DEMO_IMPACT_G if demo else IMPACT_G
    worn_col = _C.BGRN if worn else _C.BRED
    ign_col  = _C.BGRN if ign  else _C.BRED

    with _dash_lock:
        ring = list(_dash_ring)
    while len(ring) < 6:
        ring.append("")

    W = 74
    rows = [
        f"{_C.BCYN}{_C.BOLD}{'─'*W}{_C.RST}",
        (
            f"  {_C.BCYN}{_C.BOLD}SMART HELMET v5.2{_C.RST}"
            f"{'':>12}{_C.DIM}"
            f"{datetime.now().strftime('%a %d %b  %H:%M:%S')}{_C.RST}"
        ),
        f"{_C.BCYN}{'─'*W}{_C.RST}",
        (
            f"  {_C.DIM}STATE {_C.RST}"
            f"{state_col}{_C.BOLD}{state:<14}{_C.RST}"
            + (f"  {_C.BYLW}[DEMO]{_C.RST}" if demo else "")
            + (f"  {_C.BMGT}[DROWSY]{_C.RST}" if dry else "")
            + f"   {_C.DIM}RIDER {_C.RST}{_C.BWHT}{name}{_C.RST}"
            + f"  {_C.DIM}BLD {_C.RST}{blood}"
        ),
        (
            f"  {_C.DIM}HELMET {_C.RST}"
            f"{worn_col}{'WORN  ' if worn else 'OFF   '}{_C.RST}"
            f"  {_C.DIM}IGNITION {_C.RST}"
            f"{ign_col}{'ON ' if ign else 'OFF'}{_C.RST}"
            f"   {_C.DIM}STRAP {_C.RST}{'ON' if strap else 'OFF'}"
            f"  {_C.DIM}TOUCH {_C.RST}{'HI' if touch else 'LO'}"
            f"  {_C.DIM}OLED {_C.RST}{_C.BCYN}{mode}{_C.RST}"
        ),
        (
            f"  {_C.DIM}CONTACTS{_C.RST} {len(CONTACTS)}"
            f"   {_C.DIM}GPS{_C.RST} {'FIXED' if fix else 'NO FIX'}"
            f"   {_C.DIM}AI prob{_C.RST} {ai_p:.2f}"
        ),
        f"{_C.BCYN}{'─'*W}{_C.RST}",
        (
            f"  {_C.DIM}G-FORCE  {_C.RST}"
            f"{g_col}{g:5.2f} g{_C.RST}  {_C.bar(g, 5.0, 24)}"
            f"  {_C.DIM}thr{_C.RST} {thr_now:.2f} g"
        ),
        (
            f"  {_C.DIM}PITCH    {_C.RST}"
            f"{(_C.BGRN if abs(pitch) < DROWSY_PITCH else _C.BRED)}{pitch:+6.1f}°{_C.RST}"
            f"  {_C.bar(abs(pitch), 45.0, 16)}"
            f"  {_C.DIM}limit{_C.RST} {DROWSY_PITCH:.0f}°"
        ),
        (
            f"  {_C.DIM}SPEED    {_C.RST}"
            f"{_C.BCYN}{spd:5.1f} km/h{_C.RST}"
            f"   {_C.DIM}LAT{_C.RST} {lat}  {_C.DIM}LON{_C.RST} {lon}"
        ),
        f"{_C.BCYN}{'─'*W}{_C.RST}",
        f"  {_C.DIM}EVENT LOG{_C.RST}",
    ] + [f"  {r}" for r in ring] + [
        f"{_C.BCYN}{'─'*W}{_C.RST}",
    ]

    sys.stdout.write(f"\033[{_DASH_LINES}A")
    for row in rows:
        sys.stdout.write(f"\r\033[2K{row}\n")
    sys.stdout.flush()

# =============================================================================

# §19  GSM HELPERS

# =============================================================================

_gsm_lock = threading.Lock()

def _at(gsm, cmd: str, wait: float = 1.0) -> str:
    gsm.reset_input_buffer()
    gsm.write((cmd + "\r").encode())
    time.sleep(wait)
    return gsm.read_all().decode(errors="ignore")

def _network_ok(gsm) -> bool:
    r = _at(gsm, "AT+CREG?", 1)
    return ",1" in r or ",5" in r

def _signal_csq(gsm) -> int:
    try:
        r = _at(gsm, "AT+CSQ", 1)
        return int(r.split("+CSQ:")[1].split(",")[0].strip())
    except Exception:
        return 99

def gsm_init(gsm) -> None:
    display("GSM", "Initialising...", "", "")
    for cmd, wait in [
        ("AT", 1.0), ("ATE0", 1.0), ("AT+CFUN=1", 2.0),
        ('AT+CSCS="GSM"', 1.0), ("AT+CSMP=17,167,0,0", 1.0),
        ("AT+CMGF=1", 1.0), ("AT+CGATT=1", 3.0),
    ]:
        r = _at(gsm, cmd, wait)
        log.info(f"GSM {cmd!r}: {r.strip()[:60]}")
    display("GSM READY", "", "", "")
    log.info("GSM ready")

def _send_sms(gsm, number: str, msg: str) -> bool:
    for attempt in range(6):
        if _network_ok(gsm):
            break
        log.warning(f"GSM not registered ({attempt+1}/6)")
        display("GSM", f"No net {attempt+1}/6", "wait 5s", "")
        time.sleep(5)
    else:
        log.error("GSM: no network — SMS aborted")
        return False

    csq = _signal_csq(gsm)
    if csq < 5 or csq == 99:
        log.warning(f"Weak signal CSQ={csq}")

    r = _at(gsm, f'AT+CMGS="{number}"', 2.0)
    if ">" not in r:
        log.error(f"No prompt for {number}")
        return False

    gsm.write(msg.encode())
    time.sleep(0.5)
    gsm.write(b"\x1A")
    time.sleep(7)
    r2 = gsm.read_all().decode(errors="ignore")
    ok = "+CMGS" in r2
    log.info(f"SMS -> {number}: {'OK' if ok else 'FAIL'}")
    return ok

def _make_call(gsm, number: str) -> None:
    display("CALLING", number[:18], f"Ring {CALL_DURATION}s", "")
    _at(gsm, f"ATD{number};", 2.0)
    log.info(f"Call -> {number}  holding {CALL_DURATION}s")
    time.sleep(CALL_DURATION)
    _at(gsm, "ATH", 1.0)
    log.info("Call ended")

def _build_sms(lat: str, lon: str) -> str:
    with _rider_lock:
        r = dict(_rider)
    with _gps_lock:
        spd = _gps["speed"]
    peak  = _physics_fsm.take_peak()
    ts    = datetime.now().strftime("%d-%m-%Y %I:%M:%S %p")
    ai_c  = get_ai_confirmed()
    tag   = " [DEMO]" if _demo_mode else ""
    conf  = " AI-confirmed" if ai_c else " AI-unconfirmed"
    worn  = get_helmet_worn()
    return (
        f"CRASH ALERT{tag}{conf}\n"
        f"Time:     {ts}\n"
        f"G-Force:  {peak:.2f} g  |  Speed: {spd:.1f} km/h\n"
        f"Helmet:   {'Worn' if worn else 'Not worn'}\n\n"
        f"Name:     {r.get('name','-')}\n"
        f"Blood:    {r.get('blood','-')}\n"
        f"Contact:  {r.get('contact','-')}\n"
        f"Allergy:  {r.get('allergies','-')}\n"
        f"Medical:  {r.get('medical','-')}\n\n"
        f"https://maps.google.com/?q={lat},{lon}"
    )

def _log_crash(lat: str, lon: str) -> None:
    path   = CFG["crash_log"]
    exists = os.path.exists(path)
    try:
        with open(path, "a", newline="") as fh:
            w = csv.writer(fh)
            if not exists:
                w.writerow([
                    "timestamp", "lat", "lon", "speed_kmh",
                    "peak_g", "rider", "blood", "ai_confirmed",
                    "demo", "helmet_worn",
                ])
            with _rider_lock:
                name  = _rider.get("name",  "-")
                blood = _rider.get("blood", "-")
            with _gps_lock:
                spd = _gps["speed"]
            w.writerow([
                datetime.now().isoformat(), lat, lon, spd,
                round(_physics_fsm.take_peak(), 2), name, blood,
                "Y" if get_ai_confirmed() else "N",
                "Y" if _demo_mode else "N",
                "Y" if get_helmet_worn() else "N",
            ])
        log.info(f"Crash logged -> {path}")
    except Exception as exc:
        log.error(f"Crash log: {exc}")

# =============================================================================

# §20  EMERGENCY FLOW

# Runs in a dedicated _emergency_worker thread so the main loop never blocks.

# =============================================================================

_emergency_queue   = queue.Queue(maxsize=1)
_sms_dispatch_lock = threading.Lock()
_sms_dispatched    = False

def _reset_sms_dispatch() -> None:
    global _sms_dispatched
    with _sms_dispatch_lock:
        _sms_dispatched = False

def emergency(gsm) -> None:
    """
    Full emergency flow — runs inside _emergency_worker thread.
    GSM SMS + call happen here; the calling context is NOT the main thread.
    """
    global _sms_dispatched

    if not _set_state(State.CRASH_PENDING):
        crash_event.clear()
        return

    log.warning("=== EMERGENCY TRIGGERED ===")
    _dash_log(f"{_C.BRED}CRASH PENDING — {CANCEL_SEC}s cancel window{_C.RST}")
    _tele_update(crash=1)
    _tele_flush()

    # Force ignition OFF immediately
    _set_ignition(False)
    _tele_update(ignition=0)
    _dash_log(f"{_C.BRED}IGNITION FORCE OFF (crash){_C.RST}")
    log.info("Ignition forced OFF — crash state")

    # AI confirmation window (non-blocking small sleeps)
    ai_deadline = time.monotonic() + AI_CONFIRM_WIN
    while time.monotonic() < ai_deadline:
        if get_ai_confirmed():
            break
        time.sleep(0.05)

    ai_ok       = get_ai_confirmed()
    cancel_secs = CANCEL_SEC + (0 if ai_ok else CANCEL_EXTRA)
    log.info(f"AI confirmed={ai_ok}  cancel_window={cancel_secs}s")

    # Cancel countdown
    end       = time.monotonic() + cancel_secs
    cancelled = False

    while time.monotonic() < end:
        rem    = int(end - time.monotonic())
        ai_tag = "AI:YES" if ai_ok else "AI:NO "
        display("!! CRASH DETECTED !!", f"{ai_tag} Cancel:{rem}s", "Hold btn=SAFE", "")

        if GPIO.input(BUTTON_PIN) == GPIO.LOW:
            cancelled = True
            log.info("Emergency cancelled by rider")
            _dash_log(f"{_C.BGRN}Cancelled by rider{_C.RST}")
            break

        GPIO.output(BUZZER_PIN, True)
        time.sleep(0.25)
        GPIO.output(BUZZER_PIN, False)
        time.sleep(0.25)

    GPIO.output(BUZZER_PIN, False)

    if cancelled:
        _tele_update(crash=0)
        _tele_flush()
        display("CANCELLED", "Rider is safe", "", "")
        _set_state(State.NORMAL)
        _eval_ignition()   # restore ignition based on current helmet state
        time.sleep(2)
        crash_event.clear()
        _reset_ai_confirm()
        _reset_sms_dispatch()
        return

    # ── ALERTING ──────────────────────────────────────────────────────────────
    _set_state(State.ALERTING)

    with _gps_lock:
        lat   = _gps["lat"]
        lon   = _gps["lon"]
        fixed = _gps["fixed"]

    log.info(f"Alert coords: {lat},{lon}  GPS-fixed={fixed}")
    _log_crash(lat, lon)
    msg = _build_sms(lat, lon)

    with _sms_dispatch_lock:
        already_sent = _sms_dispatched
        if not already_sent:
            _sms_dispatched = True

    if already_sent:
        log.warning("SMS dispatch guard fired — duplicate blocked")
        _dash_log(f"{_C.BYLW}Duplicate emergency blocked by guard{_C.RST}")
    else:
        _dash_log(f"{_C.BYLW}Sending SMS to {len(CONTACTS)} contact(s){_C.RST}")

        with _gsm_lock:
            for number in CONTACTS:
                sent = False
                for attempt in range(1, CFG["sms_retries"] + 1):
                    display(f"SMS {attempt}/{CFG['sms_retries']}", number[:18], "", "")
                    if _send_sms(gsm, number, msg):
                        sent = True
                        _dash_log(f"{_C.BGRN}SMS sent -> {number}{_C.RST}")
                        break
                    log.warning(f"SMS attempt {attempt} failed -> {number}")
                    time.sleep(3)
                if not sent:
                    log.error(f"All SMS failed: {number}")
                    _dash_log(f"{_C.BRED}SMS FAILED -> {number}{_C.RST}")

            if CONTACTS:
                _make_call(gsm, CONTACTS[0])

    _tele_update(crash=0)
    _tele_flush()
    display("ALERTS SENT", "Help is coming", "", "")
    log.info("=== EMERGENCY COMPLETE ===")
    _dash_log(f"{_C.BGRN}Emergency complete{_C.RST}")

    _set_state(State.NORMAL)
    _eval_ignition()
    crash_event.clear()
    _reset_ai_confirm()
    _reset_sms_dispatch()

def _emergency_worker_thread(stop: threading.Event) -> None:
    """
    Dedicated thread that drains _emergency_queue and runs emergency().
    This keeps the main loop non-blocking.
    """
    while not stop.is_set():
        try:
            gsm = _emergency_queue.get(timeout=0.5)
            emergency(gsm)
        except queue.Empty:
            pass
        except Exception as exc:
            log.error(f"Emergency worker: {exc}", exc_info=True)
        _pulse("emergency_worker")

# =============================================================================

# §21  WATCHDOG

# =============================================================================

_hw_wdog = None
try:
    _hw_wdog = open("/dev/watchdog", "w")
    log.info("/dev/watchdog opened")
except Exception:
    log.info("/dev/watchdog not available — run as root to enable")

def _watchdog_thread_fn(stop: threading.Event) -> None:
    """
    Monitors all registered threads.
    Uses _restart_thread() which safely stops the old instance before spawning.
    The watchdog itself is NOT in the registry — it is never restarted.
    """
    timeout = float(CFG["watchdog_sec"])

    while not stop.is_set():
        now = time.monotonic()
        with _hb_lock:
            hb = dict(_hb)

        with _thread_reg_lock:
            names = list(_thread_registry.keys())

        for name in names:
            with _thread_reg_lock:
                entry = _thread_registry.get(name)
            if entry is None:
                continue
            _, t, _ = entry
            last   = hb.get(name, 0.0)
            alive  = (t is not None and t.is_alive())
            silent = (now - last) > timeout

            if not alive or silent:
                reason = "dead" if not alive else f"silent {now - last:.1f}s"
                log.error(f"Watchdog: '{name}' {reason} -> restarting")
                _dash_log(f"{_C.BRED}Watchdog restart: {name} ({reason}){_C.RST}")
                display("WATCHDOG", f"Restart:{name[:10]}", reason, "")
                _restart_thread(name)

        if _hw_wdog:
            try:
                _hw_wdog.write("1")
                _hw_wdog.flush()
            except Exception as exc:
                log.error(f"HW watchdog kick: {exc}")

        time.sleep(2)

# =============================================================================

# §22  STARTUP SELF-TEST

# =============================================================================

def _startup() -> None:
    print(f"\n{_C.BCYN}{_C.BOLD}")
    print("╔══════════════════════════════════════════════════════════════════╗")
    print("║        SMART HELMET v5.2  —  System Self-Test                   ║")
    print("╚══════════════════════════════════════════════════════════════════╝")
    print(_C.RST)

    display("SMART HELMET", "v5.2  Starting", "Self-test...", "")
    time.sleep(1)

    # IMU
    imu_ok = False
    try:
        _init_mpu()
        _, _, _, g, _ = _read_accel()
        imu_ok = 0.80 < g < 1.20
        col    = _C.BGRN if imu_ok else _C.BRED
        print(f"  IMU       {col}{'PASS' if imu_ok else 'FAIL'}{_C.RST}  g={g:.3f}")
        log.info(f"IMU self-test g={g:.3f}  {'PASS' if imu_ok else 'FAIL'}")
    except Exception as exc:
        print(f"  IMU       {_C.BRED}ERROR{_C.RST}  {exc}")
        log.error(f"IMU self-test: {exc}")

    # AI warm-up
    ai_ok = False
    try:
        dummy = [(0.0, 0.0, 1.0, 1.0)] * AI_BUF_SIZE
        prob  = _mlp.infer(dummy)
        ai_ok = (prob == 0.0)
        col   = _C.BGRN if ai_ok else _C.BYLW
        print(f"  AI        {col}{'PASS (pre-filter ok)' if ai_ok else 'WARN'}{_C.RST}  prob={prob:.3f}")
        log.info(f"AI self-test prob={prob:.3f}  {'PASS' if ai_ok else 'WARN'}")
    except Exception as exc:
        print(f"  AI        {_C.BRED}ERROR{_C.RST}  {exc}")
        log.error(f"AI self-test: {exc}")

    # GPIO sensor pins
    touch_r = GPIO.input(TOUCH_PIN)
    strap_r = GPIO.input(STRAP_PIN)
    menu_r  = GPIO.input(MENU_PIN)
    print(f"  TOUCH     {_C.BGRN}OK{_C.RST}  GPIO{TOUCH_PIN}={'HI' if touch_r else 'LO'}")
    print(f"  STRAP     {_C.BGRN}OK{_C.RST}  GPIO{STRAP_PIN}={'HI' if strap_r else 'LO'}")
    print(f"  MENU BTN  {_C.BGRN}OK{_C.RST}  GPIO{MENU_PIN}={'HI' if menu_r else 'LO'}")

    # Helmet state file init
    if not os.path.exists(HELMET_STATE_FILE):
        _write_helmet_state(False)
        print(f"  HELMET    {_C.BGRN}OK{_C.RST}  Created {HELMET_STATE_FILE}")

    # Buzzer check
    _beep(120, 100, 1)
    print(f"  Buzzer    {_C.BGRN}OK{_C.RST}")

    print(f"\n  {_C.DIM}THRESHOLDS{_C.RST}")
    print(f"    Normal   impact {_C.BWHT}{IMPACT_G} g{_C.RST}  "
          f"freefall {FREEFALL_G} g / {FREEFALL_MS} ms  "
          f"cooldown {CANCEL_SEC + CANCEL_EXTRA + 2}s")
    print(f"    Demo     impact {_C.BYLW}{DEMO_IMPACT_G} g{_C.RST}  "
          f"(no freefall required)")
    print(f"    AI       prob >= {AI_CRASH_PROB:.2f}  pre-filter >= {AI_PRE_FILTER} g  "
          f"confirm window {AI_CONFIRM_WIN}s")
    print(f"    Drowsy   pitch {DROWSY_PITCH}°  nods {DROWSY_NODS}x/{DROWSY_WINDOW:.0f}s  "
          f"sustain {NOD_SUSTAIN_SEC}s")
    print(f"    Cancel   {CANCEL_SEC}s (+{CANCEL_EXTRA}s if AI unconfirmed)")
    print(f"    Helmet   touch hold {TOUCH_HOLD_SEC}s  strap off {STRAP_OFF_SEC}s")
    print(f"    Debounce {GPIO_DEBOUNCE_MS} ms")
    print(f"  {_C.DIM}DEMO MODE{_C.RST}  Hold cancel {DEMO_HOLD_SEC:.0f}s OR triple-press")
    print(f"  {_C.DIM}MENU BTN {_C.RST}  Single press → cycle OLED mode (200ms debounce)")
    print()

    display(
        "SELF TEST",
        f"IMU:{'OK' if imu_ok else 'FAIL'} AI:{'OK' if ai_ok else 'WRN'}",
        f"Touch:OK Strap:OK",
        f"Thr:{IMPACT_G}g D:{DEMO_IMPACT_G}g",
    )
    time.sleep(2)

# =============================================================================

# §23  MAIN

# =============================================================================

try:
    _startup()

    gsm = serial.Serial(CFG["gsm_port"], 9600, timeout=1)
    time.sleep(2)
    gsm_init(gsm)

    # Register all restartable threads
    _THREAD_FUNCS = {
        "imu":              _imu_thread,
        "ai":               _ai_thread,
        "drowsy":           _drowsy_thread,
        "demo_watch":       _demo_watch_thread,
        "gps":              _gps_thread,
        "rider":            _rider_thread,
        "oled":             _oled_thread,
        "telemetry":        _telemetry_thread,
        "dashboard":        _dashboard_thread,
        "helmet":           _helmet_thread,
        "menu":             _menu_thread,
        "emergency_worker": _emergency_worker_thread,
    }

    for name, fn in _THREAD_FUNCS.items():
        _register_and_start(name, fn)

    # Watchdog runs outside the registry (never restarted by itself)
    _wd_stop = threading.Event()
    threading.Thread(
        target=_watchdog_thread_fn,
        args=(_wd_stop,),
        name="watchdog",
        daemon=True,
    ).start()

    display(
        "HELMET ACTIVE",
        f"Thr:{IMPACT_G}g AI:{AI_CRASH_PROB:.0%}",
        "5s hold=DEMO  Menu=cycle",
        "",
    )
    log.info(
        f"Smart Helmet v5.2 active | contacts={len(CONTACTS)}"
        f" | touch={TOUCH_PIN} strap={STRAP_PIN} menu={MENU_PIN}"
        f" | touch_hold={TOUCH_HOLD_SEC}s strap_off={STRAP_OFF_SEC}s"
        f" | debounce={GPIO_DEBOUNCE_MS}ms"
        f" | nod_sustain={NOD_SUSTAIN_SEC}s"
        f" | watchdog_sec={CFG['watchdog_sec']}"
    )
    _dash_log(f"{_C.BGRN}System ready — {len(CONTACTS)} contact(s){_C.RST}")

    # ── Main loop — posts crash work to emergency_worker, never blocks ────────
    while True:
        if crash_event.wait(timeout=1.0):
            if get_state() == State.NORMAL:
                # Post to worker queue; drop silently if already queued
                try:
                    _emergency_queue.put_nowait(gsm)
                except queue.Full:
                    log.warning("Emergency already queued — ignoring duplicate trigger")
            else:
                log.warning(f"crash_event fired in state={get_state()} — cleared")
                crash_event.clear()

except KeyboardInterrupt:
    log.info("Stopped by user (SIGINT)")
    print(f"\n{_C.BYLW}Stopped by user.{_C.RST}\n")

except Exception as exc:
    log.critical(f"Fatal: {exc}", exc_info=True)
    display("FATAL ERROR", str(exc)[:20], "See helmet.log", "")
    print(f"\n{_C.BRED}FATAL: {exc}{_C.RST}\n")

finally:
    # Signal all registered threads to stop
    with _thread_reg_lock:
        entries = list(_thread_registry.values())
        for _, t, stop_ev in entries:
            stop_ev.set()

    # Signal watchdog
    try:
        _wd_stop.set()
    except NameError:
        pass

    GPIO.output(BUZZER_PIN, False)
    GPIO.cleanup()

    if _hw_wdog:
        try:
            _hw_wdog.write("V")
            _hw_wdog.close()
            log.info("Hardware watchdog disarmed")
        except Exception:
            pass

    _tele_update(crash=0, demo=0, drowsy=0, helmet=0, ignition=0)
    _tele_flush()
    _write_helmet_state(False)

    display("STOPPED", "Goodbye", "", "")
    log.info("Shutdown complete")
    print(f"{_C.BCYN}Shutdown complete.{_C.RST}\n")
