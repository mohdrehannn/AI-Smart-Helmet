

import math, os, time, json, csv, logging, threading, smbus, serial
import RPi.GPIO as GPIO
from datetime import datetime

# ─────────────────────────────────────────────────────────────
# LOGGING  (file only — avoids blocking stdout on Pi Zero)
# ─────────────────────────────────────────────────────────────
logging.basicConfig(
    filename="helmet.log",
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s [%(threadName)s] %(message)s",
)
log = logging.getLogger()

# ─────────────────────────────────────────────────────────────
# CONFIG  (edit config.json — never edit this file)
# ─────────────────────────────────────────────────────────────
_DEFAULTS = {
    "impact_g":         3.15,   # g-force that confirms an impact
    "freefall_g":       0.40,   # g-force below which freefall begins
    "freefall_ms":      80,     # min freefall duration (ms) before impact counts
    "cancel_sec":       10,     # seconds the rider has to cancel the alert
    "buzzer_pin":       17,
    "button_pin":       18,
    "mpu_addr":         "0x69",
    "gsm_port":         "/dev/ttyS0",
    "gps_port":         "/dev/ttyUSB0",
    "contacts":         ["+919494844078"],
    "rider_file":       "rider.json",
    "ble_file":         "ble_status.txt",
    "default_lat":      "17.3422348",
    "default_lon":      "78.3674875",
    "crash_log":        "crash_log.csv",
    "sms_retries":      3,
    "call_duration":    25,     # seconds to hold the voice call open
    "watchdog_sec":     5.0,    # seconds of silence before a thread is restarted
}

def _load_cfg():
    cfg = _DEFAULTS.copy()
    if os.path.exists("config.json"):
        try:
            with open("config.json") as f:
                cfg.update(json.load(f))
        except Exception as e:
            log.error(f"config.json load error: {e} — using defaults")
    else:
        with open("config.json", "w") as f:
            json.dump(_DEFAULTS, f, indent=2)
        log.info("Default config.json written")
    return cfg

CFG = _load_cfg()

IMPACT_G    = float(CFG["impact_g"])
FREEFALL_G  = float(CFG["freefall_g"])
FREEFALL_MS = int(CFG["freefall_ms"])
CANCEL_SEC  = int(CFG["cancel_sec"])
BUZZER      = int(CFG["buzzer_pin"])
BUTTON      = int(CFG["button_pin"])
MPU_ADDR    = int(CFG["mpu_addr"], 16)
CONTACTS    = CFG["contacts"]

# ─────────────────────────────────────────────────────────────
# GPIO
# ─────────────────────────────────────────────────────────────
GPIO.setmode(GPIO.BCM)
GPIO.setup(BUZZER, GPIO.OUT)
GPIO.setup(BUTTON, GPIO.IN, pull_up_down=GPIO.PUD_UP)
GPIO.output(BUZZER, 0)

# ─────────────────────────────────────────────────────────────
# OLED  — isolated lock; errors never crash the caller
# ─────────────────────────────────────────────────────────────
import board, busio
from PIL import Image, ImageDraw
import adafruit_ssd1306

_oled_lock = threading.Lock()
_i2c       = busio.I2C(board.SCL, board.SDA)
_oled      = adafruit_ssd1306.SSD1306_I2C(128, 64, _i2c, addr=0x3C)

def display(l1="", l2="", l3="", l4=""):
    """Thread-safe 4-line OLED write.  Silent on failure — never raises."""
    try:
        with _oled_lock:
            img  = Image.new("1", (128, 64))
            draw = ImageDraw.Draw(img)
            for row, text in enumerate([l1, l2, l3, l4]):
                draw.text((0, row * 16), str(text)[:21], fill=255)
            _oled.image(img)
            _oled.show()
    except Exception as e:
        log.error(f"OLED: {e}")

# ─────────────────────────────────────────────────────────────
# SHARED STATE
#
# Design rule: each sub-dict is written by exactly ONE thread.
#   _gps   → written by gps_thread   (protected by _gps_lock)
#   _rider → written by rider_thread  (protected by _rider_lock)
#   _g_now → written by imu_thread    (Python float; GIL makes
#             reads/writes atomic — no lock needed on hot path)
#
# crash_event is a threading.Event — the only cross-thread
# signal used.  set() is atomic; wait() is blocking & efficient.
# ─────────────────────────────────────────────────────────────
_gps_lock   = threading.Lock()
_rider_lock = threading.Lock()

_gps = {
    "lat":   CFG["default_lat"],
    "lon":   CFG["default_lon"],
    "speed": 0.0,
    "fixed": False,
}
_rider = {
    "name": "Unknown", "blood": "-",
    "contact": "-",    "allergies": "-",
    "medical": "-",
}

_g_now       = 0.0          # float; GIL-safe single-writer/multi-reader
crash_event  = threading.Event()

# ─────────────────────────────────────────────────────────────
# WATCHDOG HEARTBEAT
# Each thread calls pulse(name) inside its loop.
# The watchdog thread checks timestamps and restarts dead threads.
# ─────────────────────────────────────────────────────────────
_hb      = {}                   # {thread_name: monotonic timestamp}
_hb_lock = threading.Lock()

def pulse(name: str) -> None:
    with _hb_lock:
        _hb[name] = time.monotonic()

# ─────────────────────────────────────────────────────────────
# IMU — MPU6050
# ─────────────────────────────────────────────────────────────
_bus = smbus.SMBus(1)

def _init_mpu() -> None:
    _bus.write_byte_data(MPU_ADDR, 0x6B, 0x00)  # wake, no sleep
    _bus.write_byte_data(MPU_ADDR, 0x1C, 0x00)  # accel ±2g
    _bus.write_byte_data(MPU_ADDR, 0x1A, 0x03)  # DLPF 44 Hz — smooths noise
    log.info("MPU6050 initialised (DLPF 44 Hz, ±2g)")

def _read_word(reg: int) -> int:
    h = _bus.read_byte_data(MPU_ADDR, reg)
    l = _bus.read_byte_data(MPU_ADDR, reg + 1)
    v = (h << 8) | l
    return v - 65536 if v >= 0x8000 else v

def _get_g() -> float:
    ax = _read_word(0x3B) / 16384.0
    ay = _read_word(0x3D) / 16384.0
    az = _read_word(0x3F) / 16384.0
    return math.sqrt(ax*ax + ay*ay + az*az)

# ─────────────────────────────────────────────────────────────
# CRASH DETECTOR  (freefall → impact state machine)
#
# States: IDLE → FREEFALL → (impact? CRASH : back to IDLE)
#
# __slots__ eliminates per-instance __dict__ overhead — keeps
# this tight since feed() is called 50× per second.
# ─────────────────────────────────────────────────────────────
class _CrashDetector:
    __slots__ = ("_in_ff", "_ff_start", "_peak")

    def __init__(self):
        self._in_ff   = False
        self._ff_start = 0.0
        self._peak     = 0.0

    def feed(self, g: float) -> bool:
        """Return True exactly once when a crash is confirmed."""
        if g > self._peak:
            self._peak = g

        if not self._in_ff:
            if g < FREEFALL_G:
                self._in_ff    = True
                self._ff_start = time.monotonic()
        else:
            elapsed_ms = (time.monotonic() - self._ff_start) * 1000.0
            if g > IMPACT_G and elapsed_ms >= FREEFALL_MS:
                self._in_ff = False
                log.warning(f"CRASH CONFIRMED: {g:.2f}g after {elapsed_ms:.0f}ms freefall")
                return True
            if g >= FREEFALL_G:
                # Recovered without impact — false alarm
                self._in_ff = False
        return False

    def take_peak(self) -> float:
        """Read and reset the rolling peak g value."""
        p, self._peak = self._peak, 0.0
        return p

_detector = _CrashDetector()

# ─────────────────────────────────────────────────────────────
# THREAD: IMU  (50 Hz, elevated OS priority)
#
# This thread has one job: read g-force and set crash_event.
# It writes _g_now (GIL-safe float) so the OLED thread can
# display the current value without any lock overhead.
# ─────────────────────────────────────────────────────────────
def imu_thread_fn():
    global _g_now
    try:
        # os.nice(-10) gives this thread a higher scheduler priority.
        # Requires running the process with sudo; silently ignored otherwise.
        os.nice(-10)
    except Exception:
        pass

    while True:
        try:
            g      = _get_g()
            _g_now = g                          # GIL-safe atomic float write

            if _detector.feed(g) and not crash_event.is_set():
                crash_event.set()               # wake main thread immediately

        except Exception as e:
            log.error(f"IMU read: {e}")

        pulse("imu")
        time.sleep(0.02)                        # 50 Hz

# ─────────────────────────────────────────────────────────────
# THREAD: GPS  (background; crash path uses cached value only)
#
# The emergency() function NEVER waits for GPS.  It reads
# _gps immediately.  The last known position is always better
# than a multi-second blocking wait during an emergency.
# ─────────────────────────────────────────────────────────────
def _parse_gprmc(line: str):
    """Parse $GPRMC → (lat, lon, speed_kmh) or raise."""
    p   = line.split(",")
    if p[2] != "A":
        raise ValueError("No fix")
    lat = float(p[3]); lon = float(p[5])
    lat = int(lat / 100) + (lat % 100) / 60.0
    lon = int(lon / 100) + (lon % 100) / 60.0
    spd = float(p[7]) * 1.852 if p[7] else 0.0
    return round(lat, 6), round(lon, 6), round(spd, 1)

def gps_thread_fn():
    while True:
        try:
            ser = serial.Serial(CFG["gps_port"], 9600, timeout=1)
            log.info("GPS serial open")
            while True:
                try:
                    line = ser.readline().decode(errors="ignore")
                except Exception:
                    break                       # serial error → reopen
                if "$GPRMC" not in line:
                    continue
                try:
                    lat, lon, spd = _parse_gprmc(line)
                    with _gps_lock:
                        _gps["lat"]   = str(lat)
                        _gps["lon"]   = str(lon)
                        _gps["speed"] = spd
                        _gps["fixed"] = True
                except Exception:
                    pass                        # malformed NMEA — skip silently
                pulse("gps")
            ser.close()
        except Exception as e:
            log.error(f"GPS serial: {e}")
        time.sleep(5)                           # back-off before reopen

# ─────────────────────────────────────────────────────────────
# THREAD: RIDER / BLE  (reads JSON every 3 s)
# ─────────────────────────────────────────────────────────────
def rider_thread_fn():
    while True:
        # Rider profile
        try:
            with open(CFG["rider_file"]) as f:
                data = json.load(f)
            with _rider_lock:
                _rider.update(data)
        except Exception:
            pass                                # keep previous values

        pulse("rider")
        time.sleep(3)

# ─────────────────────────────────────────────────────────────
# THREAD: OLED  (single screen, 2 s refresh)
#
# One screen only.  No PIL font loading.  Any exception is
# caught inside display() — this thread cannot crash the system.
# ─────────────────────────────────────────────────────────────
def oled_thread_fn():
    while True:
        try:
            ble = "?"
            try:
                with open(CFG["ble_file"]) as f:
                    ble = f.read().strip()[:4]
            except Exception:
                pass

            g   = _g_now                        # GIL-safe float read
            with _gps_lock:
                spd   = _gps["speed"]
                fixed = _gps["fixed"]
            with _rider_lock:
                name = _rider.get("name", "Unknown")[:16]

            display(
                f"G:{g:.2f}g  BT:{ble}",
                name,
                f"Spd:{spd}km  GPS:{'Y' if fixed else 'N'}",
                datetime.now().strftime("%H:%M:%S"),
            )
        except Exception as e:
            log.error(f"OLED thread: {e}")

        pulse("oled")
        time.sleep(2)

# ─────────────────────────────────────────────────────────────
# GSM HELPERS
# All GSM calls go through _gsm_lock so threads never
# share the serial port simultaneously.
# ─────────────────────────────────────────────────────────────
_gsm_lock = threading.Lock()

def _at(gsm, cmd: str, wait: float = 1.0) -> str:
    """Send one AT command; return decoded response."""
    gsm.reset_input_buffer()
    gsm.write((cmd + "\r").encode())
    time.sleep(wait)
    return gsm.read_all().decode(errors="ignore")

def _network_ok(gsm) -> bool:
    """True when GSM is registered (home or roaming)."""
    r = _at(gsm, "AT+CREG?", 1)
    return ",1" in r or ",5" in r

def _signal_level(gsm) -> int:
    """Return CSQ value 0-31 (99 = unknown/no signal)."""
    try:
        r = _at(gsm, "AT+CSQ", 1)
        return int(r.split("+CSQ:")[1].split(",")[0].strip())
    except Exception:
        return 99

def gsm_init(gsm) -> None:
    display("GSM", "Initialising...", "", "")
    seq = [
        ("AT",                 1.0),
        ("ATE0",               1.0),
        ("AT+CFUN=1",          2.0),
        ('AT+CSCS="GSM"',      1.0),
        ("AT+CSMP=17,167,0,0", 1.0),
        ("AT+CMGF=1",          1.0),
        ("AT+CGATT=1",         3.0),
    ]
    for cmd, wait in seq:
        r = _at(gsm, cmd, wait)
        log.info(f"GSM {cmd!r}: {r.strip()[:60]}")
    display("GSM READY", "", "", "")
    log.info("GSM initialised")

def _send_sms(gsm, number: str, msg: str) -> bool:
    """
    Send one SMS.
    Pre-checks:
      • Network registration (AT+CREG?) — waits up to 30 s
      • Signal quality    (AT+CSQ)     — warns if weak
    Confirms delivery with +CMGS in response.
    Returns True on confirmed success.
    """
    # Wait for network registration (up to 30 s)
    for attempt in range(6):
        if _network_ok(gsm):
            break
        log.warning(f"GSM: not registered, retry {attempt+1}/6")
        display("GSM", f"No network {attempt+1}/6", "waiting 5s...", "")
        time.sleep(5)
    else:
        log.error("GSM: network unavailable — SMS aborted")
        return False

    sig = _signal_level(gsm)
    log.info(f"GSM signal (CSQ): {sig}")
    if sig < 5 or sig == 99:
        log.warning(f"GSM: weak signal CSQ={sig} — proceeding anyway")

    r = _at(gsm, f'AT+CMGS="{number}"', 2.0)
    if ">" not in r:
        log.error(f"No '>' prompt for {number}: {r[:60]}")
        return False

    gsm.write(msg.encode())
    time.sleep(0.5)
    gsm.write(b"\x1A")         # Ctrl-Z — transmit SMS
    time.sleep(7)
    r2  = gsm.read_all().decode(errors="ignore")
    ok  = "+CMGS" in r2
    log.info(f"SMS → {number}: {'OK' if ok else 'FAIL'} | {r2[:60]}")
    return ok

def _make_call(gsm, number: str) -> None:
    display("CALLING", number[:20], "", "")
    _at(gsm, f"ATD{number};", 2.0)
    log.info(f"Call to {number}, holding {CFG['call_duration']}s")
    time.sleep(CFG["call_duration"])
    _at(gsm, "ATH", 1.0)
    log.info("Call ended")

# ─────────────────────────────────────────────────────────────
# SMS MESSAGE BUILDER
# ─────────────────────────────────────────────────────────────
def _build_sms(lat: str, lon: str) -> str:
    with _rider_lock:
        r = _rider.copy()
    with _gps_lock:
        spd = _gps["speed"]
    peak = _detector.take_peak()
    ts   = datetime.now().strftime("%d-%m-%Y %I:%M:%S %p")
    return (
        f"CRASH ALERT\n"
        f"Time: {ts}\n"
        f"G-Force: {peak:.2f}g | Speed: {spd} km/h\n\n"
        f"Name:     {r.get('name','-')}\n"
        f"Blood:    {r.get('blood','-')}\n"
        f"Contact:  {r.get('contact','-')}\n"
        f"Allergies:{r.get('allergies','-')}\n"
        f"Medical:  {r.get('medical','-')}\n\n"
        f"https://maps.google.com/?q={lat},{lon}"
    )

# ─────────────────────────────────────────────────────────────
# CRASH LOG  (CSV, append-only)
# ─────────────────────────────────────────────────────────────
def _log_crash(lat: str, lon: str) -> None:
    path   = CFG["crash_log"]
    exists = os.path.exists(path)
    try:
        with open(path, "a", newline="") as f:
            w = csv.writer(f)
            if not exists:
                w.writerow(["timestamp","lat","lon","speed_kmh","peak_g",
                             "rider","blood"])
            with _rider_lock:
                name  = _rider.get("name",  "-")
                blood = _rider.get("blood", "-")
            with _gps_lock:
                spd = _gps["speed"]
            w.writerow([datetime.now().isoformat(), lat, lon, spd,
                        round(_detector._peak, 2), name, blood])
        log.info(f"Crash written to {path}")
    except Exception as e:
        log.error(f"Crash log: {e}")

# ─────────────────────────────────────────────────────────────
# EMERGENCY FLOW  (called from main thread only)
#
# Critical path:
#  1. Give rider CANCEL_SEC seconds to press the button
#  2. Read cached GPS  ← no blocking, zero latency
#  3. SMS all contacts with retry + backoff
#  4. Voice call primary contact
# ─────────────────────────────────────────────────────────────
def emergency(gsm) -> None:
    log.warning("=== EMERGENCY TRIGGERED ===")
    end       = time.monotonic() + CANCEL_SEC
    cancelled = False

    while time.monotonic() < end:
        rem = int(end - time.monotonic())
        display("!! CRASH DETECTED !!", f"Cancel in: {rem}s", "Press btn=SAFE", "")

        if GPIO.input(BUTTON) == 0:
            cancelled = True
            log.info("Emergency cancelled by rider")
            break

        GPIO.output(BUZZER, 1); time.sleep(0.25)
        GPIO.output(BUZZER, 0); time.sleep(0.25)

    GPIO.output(BUZZER, 0)

    if cancelled:
        display("CANCELLED", "Rider is safe", "", "")
        time.sleep(2)
        crash_event.clear()
        return

    # Snapshot GPS immediately — no waiting
    with _gps_lock:
        lat   = _gps["lat"]
        lon   = _gps["lon"]
        fixed = _gps["fixed"]

    log.info(f"Alert coords: {lat},{lon}  GPS-fixed={fixed}")
    _log_crash(lat, lon)
    msg = _build_sms(lat, lon)

    with _gsm_lock:
        for number in CONTACTS:
            sent = False
            for attempt in range(1, CFG["sms_retries"] + 1):
                display(f"SMS {attempt}/{CFG['sms_retries']}", number[:20], "", "")
                if _send_sms(gsm, number, msg):
                    sent = True
                    break
                log.warning(f"SMS attempt {attempt} failed for {number}, retrying...")
                time.sleep(3)
            if not sent:
                log.error(f"All {CFG['sms_retries']} SMS attempts failed: {number}")

        if CONTACTS:
            _make_call(gsm, CONTACTS[0])

    display("ALERTS SENT", "Help is coming", "", "")
    log.info("=== EMERGENCY COMPLETE ===")
    crash_event.clear()

# ─────────────────────────────────────────────────────────────
# WATCHDOG  (software + hardware /dev/watchdog)
#
# Software: checks each thread's heartbeat timestamp.
#   If a thread has been silent for watchdog_sec, it is
#   considered hung and is restarted via _spawn().
#
# Hardware: writes "1" to /dev/watchdog every cycle.
#   If the watchdog thread itself dies, the kernel reboots
#   the Pi after the hardware timeout (~60 s default).
#   Disarmed with "V" on clean shutdown.
# ─────────────────────────────────────────────────────────────
_hw_wdog = None
try:
    _hw_wdog = open("/dev/watchdog", "w")
    log.info("Hardware watchdog opened (/dev/watchdog)")
except Exception:
    log.info("/dev/watchdog not available (run as root to enable)")

# Registry of restartable threads
_THREAD_FNS: dict = {}      # populated after function definitions
_threads:    dict = {}

def _spawn(name: str) -> threading.Thread:
    t = threading.Thread(
        target=_THREAD_FNS[name], name=name, daemon=True
    )
    t.start()
    _threads[name] = t
    log.info(f"Thread '{name}' started (ident={t.ident})")
    return t

def watchdog_thread_fn() -> None:
    timeout = float(CFG["watchdog_sec"])
    while True:
        now = time.monotonic()
        with _hb_lock:
            hb = dict(_hb)

        for name in _THREAD_FNS:
            last = hb.get(name, 0.0)
            t    = _threads.get(name)
            if (now - last > timeout) or (t is not None and not t.is_alive()):
                log.error(f"Watchdog: thread '{name}' unresponsive — restarting")
                display("WATCHDOG", f"Restart:{name}", "", "")
                _spawn(name)

        # Kick hardware watchdog
        if _hw_wdog:
            try:
                _hw_wdog.write("1")
                _hw_wdog.flush()
            except Exception as e:
                log.error(f"HW watchdog kick failed: {e}")

        time.sleep(2)

# ─────────────────────────────────────────────────────────────
# STARTUP SELF-TEST
# ─────────────────────────────────────────────────────────────
def startup() -> None:
    display("SMART HELMET", "v3.0  Reliability", "Booting...", "")
    time.sleep(2)

    # IMU self-test: at rest should read ~1.0g (gravity)
    imu_ok = False
    try:
        _init_mpu()
        g      = _get_g()
        imu_ok = 0.80 < g < 1.20
        log.info(f"IMU self-test: g={g:.3f}  {'PASS' if imu_ok else 'FAIL'}")
    except Exception as e:
        log.error(f"IMU self-test error: {e}")

    # Buzzer beep confirms GPIO is working
    GPIO.output(BUZZER, 1); time.sleep(0.1); GPIO.output(BUZZER, 0)

    display(
        "SELF TEST",
        f"IMU: {'PASS' if imu_ok else 'FAIL'}",
        "Buzzer: OK",
        f"Threshold: {IMPACT_G}g",
    )
    time.sleep(2)

# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────
try:
    startup()

    gsm = serial.Serial(CFG["gsm_port"], 9600, timeout=1)
    time.sleep(2)
    gsm_init(gsm)

    # Register restartable threads (watchdog needs this dict)
    _THREAD_FNS.update({
        "imu":   imu_thread_fn,
        "gps":   gps_thread_fn,
        "rider": rider_thread_fn,
        "oled":  oled_thread_fn,
    })

    # Spawn worker threads
    for name in _THREAD_FNS:
        _spawn(name)

    # Watchdog is NOT in _THREAD_FNS — if it dies, the hardware
    # watchdog reboots the Pi.
    threading.Thread(
        target=watchdog_thread_fn, name="watchdog", daemon=True
    ).start()

    display("HELMET ACTIVE", f"Threshold:{IMPACT_G}g", "", "")
    log.info(f"Smart Helmet v3.0 active | contacts={len(CONTACTS)}")

    # ── Main thread: only unblocks on crash events ──────────────
    # threading.Event.wait() releases the GIL completely — the
    # IMU thread runs at full speed with zero contention.
    while True:
        if crash_event.wait(timeout=1.0):
            emergency(gsm)

except KeyboardInterrupt:
    log.info("Stopped by user (SIGINT)")

except Exception as e:
    log.critical(f"Fatal: {e}", exc_info=True)
    display("FATAL ERROR", str(e)[:20], "See helmet.log", "")

finally:
    GPIO.output(BUZZER, 0)
    GPIO.cleanup()

    # Disarm hardware watchdog gracefully ("V" = magic close)
    if _hw_wdog:
        try:
            _hw_wdog.write("V")
            _hw_wdog.close()
            log.info("Hardware watchdog disarmed")
        except Exception:
            pass

    display("STOPPED", "Goodbye", "", "")
    log.info("Shutdown complete")
