<div align="center">

```
███████╗███╗   ███╗ █████╗ ██████╗ ████████╗    ██╗  ██╗███████╗██╗      ███╗   ███╗███████╗████████╗
██╔════╝████╗ ████║██╔══██╗██╔══██╗╚══██╔══╝    ██║  ██║██╔════╝██║      ████╗ ████║██╔════╝╚══██╔══╝
███████╗██╔████╔██║███████║██████╔╝   ██║       ███████║█████╗  ██║      ██╔████╔██║█████╗     ██║
╚════██║██║╚██╔╝██║██╔══██║██╔══██╗   ██║       ██╔══██║██╔══╝  ██║      ██║╚██╔╝██║██╔══╝     ██║
███████║██║ ╚═╝ ██║██║  ██║██║  ██║   ██║       ██║  ██║███████╗███████╗ ██║ ╚═╝ ██║███████╗   ██║
╚══════╝╚═╝     ╚═╝╚═╝  ╚═╝╚═╝  ╚═╝   ╚═╝       ╚═╝  ╚═╝╚══════╝╚══════╝ ╚═╝     ╚═╝╚══════╝   ╚═╝
```

# AI Smart Helmet

### Intelligent Accident Detection & Emergency Alert System

*Powered by Raspberry Pi Zero 2W · Built by Rehan*

-----

[![Python](https://img.shields.io/badge/Python-3.9+-3776AB?style=flat-square&logo=python&logoColor=white)](https://python.org)
[![Platform](https://img.shields.io/badge/Platform-Raspberry%20Pi%20Zero%202W-C51A4A?style=flat-square&logo=raspberry-pi&logoColor=white)](https://www.raspberrypi.com/)
[![License](https://img.shields.io/badge/License-MIT-22c55e?style=flat-square)](LICENSE)
[![Status](https://img.shields.io/badge/Status-Active%20Development-f59e0b?style=flat-square)]()
[![University](https://img.shields.io/badge/Institution-Lords%20Institute%20of%20Engineering-6366f1?style=flat-square)]()

</div>

-----

## Overview

The **AI Smart Helmet** is an embedded safety system for motorcycle riders that combines real-time crash physics detection, drowsiness monitoring, GPS tracking, and automated emergency communication — all running on a Raspberry Pi Zero 2W mounted inside a helmet.

When a crash is detected, the system:

1. Triggers a **cancellable alert** (the rider has a grace window to dismiss false triggers)
1. Sends an **emergency SMS** with rider details, G-force data, and a live Google Maps link
1. **Places an automatic call** to the registered emergency contact

The system runs 10 concurrent threads with a hardware watchdog, a strict finite state machine, and a layered detection pipeline — making it resilient to false positives while remaining sensitive to genuine emergencies.

-----

## Table of Contents

- [System Architecture](#system-architecture)
- [Features](#features)
- [Hardware](#hardware)
- [Software Stack](#software-stack)
- [Detection Logic](#detection-logic)
- [Thread Map](#thread-map)
- [Getting Started](#getting-started)
- [Configuration](#configuration)
- [Demo Mode](#demo-mode)
- [Project Structure](#project-structure)
- [Team](#team)

-----

## System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     Raspberry Pi Zero 2W                        │
│                                                                 │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐  │
│  │ IMU      │    │ AI/MLP   │    │ Drowsy   │    │ GPS      │  │
│  │ 50 Hz    │───▶│ 5 Hz     │    │ 10 Hz    │    │ Event    │  │
│  │ Physics  │    │ Confirm  │    │ Head Nod │    │ Driven   │  │
│  │ FSM      │    │ Only     │    │ FSM      │    │          │  │
│  └────┬─────┘    └──────────┘    └──────────┘    └────┬─────┘  │
│       │                                               │        │
│       ▼  crash_event (threading.Event)                ▼        │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │                  MAIN THREAD (emergency)                 │  │
│  │  NORMAL ──▶ CRASH_PENDING ──▶ ALERTING ──▶ NORMAL       │  │
│  └──────────────────────────────────────────────────────────┘  │
│       │                                                        │
│       ▼                                                        │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐                  │
│  │ SIM800L  │    │ OLED     │    │ BLE      │                  │
│  │ SMS+Call │    │ Display  │    │Telemetry │                  │
│  └──────────┘    └──────────┘    └──────────┘                  │
└─────────────────────────────────────────────────────────────────┘
```

-----

## Features

### 🔴 Crash Detection (Physics FSM)

- Freefall → high-G impact detection using MPU6050
- 3-sample moving-average smoothing eliminates single-sample road noise
- Freefall must be **sustained** for the full configured window — pothole spikes are rejected
- Full-window cooldown prevents re-triggering during active emergency handling

### 🤖 AI Confirmation Layer (TinyML MLP)

- Lightweight 10→8→1 numpy MLP runs at 5 Hz as a **confirmation layer only**
- The AI **never** triggers a crash directly — it only confirms after physics fires
- Pre-filter skips inference entirely at normal 1g riding (zero false-positive risk)
- If AI does not confirm within 2 seconds, the cancel window is extended by 5 seconds

### 😴 Drowsiness Detection

- Head-nod pitch FSM detects repeated forward head dips
- **Sustain filter**: a nod is only counted if the head stays forward-tilted for ≥ 0.4 seconds — vibration spikes (< 150 ms) are silently ignored
- Distinct buzzer pattern (200 ms on / 300 ms off) for drowsiness vs crash
- Auto-clears after 3 seconds of upright posture

### 📍 GPS Tracking

- Real-time location via NEO-7M NMEA parsing
- Falls back to configurable default coordinates if no fix is available
- Speed, latitude, and longitude included in every emergency SMS

### 📱 Emergency Communication (SIM800L)

- SMS includes: timestamp, G-force, speed, GPS map link, rider name, blood type, allergies, and medical notes
- Configurable retry count per contact (default: 3 attempts)
- Sends to multiple contacts simultaneously
- Automatic voice call to primary contact after SMS dispatch

### 🛡️ SMS Deduplication

- Lock-guarded `_sms_dispatched` flag ensures exactly one SMS volley per crash event regardless of threading edge cases
- Re-armed automatically after each complete crash cycle

### 📺 OLED Display

- Live 4-line status: G-force, rider name, speed, GPS fix, and current state
- Updates every 2 seconds — lightweight I2C refresh

### 📡 BLE Telemetry

- JSON telemetry file flushed every second for companion Flutter app
- Atomic write via temp file + `os.replace()` — no partial reads

### 🔁 Watchdog & Thread Supervision

- Software watchdog monitors all 9 threads by heartbeat
- Hardware `/dev/watchdog` kicked every 2 seconds
- Dead or silent threads are automatically restarted
- 8-second timeout prevents false restarts on Pi Zero 2W CPU spikes

### 🎮 Demo Mode

- Intentionally relaxed trigger: any smoothed G ≥ `demo_impact_g` (1.30g) fires instantly
- No freefall window required — suitable for live presentations and testing
- Toggle: hold cancel button for 5 seconds, OR triple-press within 2 seconds
- Visually distinct on OLED and dashboard; clearly tagged in SMS and crash log

-----

## Hardware

|Category      |Component                      |Purpose                                 |
|--------------|-------------------------------|----------------------------------------|
|**Processing**|Raspberry Pi Zero 2W           |Main compute unit                       |
|**IMU**       |MPU6050                        |Accelerometer + Gyroscope (I2C)         |
|**GSM**       |SIM800L                        |Emergency SMS and voice call            |
|**GPS**       |NEO-7M                         |Real-time location tracking             |
|**Bluetooth** |Built-in (Pi Zero 2W)          |BLE telemetry to companion app          |
|**Display**   |SSD1306 OLED 128×64            |Live helmet status display (I2C)        |
|**Buzzer**    |Passive buzzer                 |Crash and drowsiness alerts             |
|**Input**     |Push button                    |Cancel alert / toggle demo mode         |
|**Power**     |Li-ion battery                 |Primary power source                    |
|**GSM Power** |Dedicated supply               |Handles SIM800L current spikes (2A peak)|
|**Regulation**|Voltage regulators + capacitors|Stable 3.3V/5V rails                    |
|**USB–UART**  |CP2102 converter               |GPS serial connection                   |
|**USB Hub**   |USB hub                        |Multiple peripheral support             |

### Wiring Summary

```
Pi Zero 2W GPIO
├── GPIO 17  →  Buzzer (active high)
├── GPIO 18  →  Push Button (pull-up, active low)
├── I2C SDA  →  MPU6050 SDA + SSD1306 SDA
├── I2C SCL  →  MPU6050 SCL + SSD1306 SCL
├── /dev/ttyS0   →  SIM800L TX/RX
└── /dev/ttyUSB0 →  NEO-7M GPS (via USB–UART)
```

-----

## Software Stack

|Layer        |Technology                                     |
|-------------|-----------------------------------------------|
|Language     |Python 3.9+                                    |
|IMU          |`smbus` (direct I2C register reads)            |
|OLED         |`adafruit-circuitpython-ssd1306`, `Pillow`     |
|GPS          |`pyserial` (NMEA GPRMC parsing)                |
|GSM          |`pyserial` (AT command set)                    |
|GPIO         |`RPi.GPIO`                                     |
|AI/ML        |`numpy` (MLP forward pass only — no frameworks)|
|Threading    |`threading` (standard library)                 |
|Companion App|Flutter (BLE, reads `telemetry.json`)          |

-----

## Detection Logic

### Crash Detection Pipeline

```
Raw IMU @ 50 Hz
      │
      ▼
  _smooth_g()          ← 3-sample moving average (kills road noise)
      │
      ▼
 _PhysicsFSM.feed()
      │
      ├── DEMO MODE?   ── sg >= DEMO_IMPACT_G? ──▶ crash_event.set()
      │
      └── NORMAL MODE
              │
              ├── sg < FREEFALL_G?  ──▶  start freefall timer
              │
              └── sg > IMPACT_G AND elapsed >= FREEFALL_MS?
                          │
                          └──▶  crash_event.set()   ← ONLY trigger point
                                      │
                                      ▼
                               _ai_thread (5 Hz)
                               confirms within 2 s?
                                      │
                              YES ────┴──── NO
                               │             │
                          10 s window    15 s window
                               │             │
                          Cancel btn?   Cancel btn?
                               │             │
                              NO            NO
                               └─────┬───────┘
                                     ▼
                                 SMS + Call
```

### Drowsiness Detection Pipeline

```
pitch_now (from IMU @ 10 Hz via drowsy_thread)
      │
      ▼
 pitch > DROWSY_PITCH (25°)?
      │
      YES ──▶  record _nod_enter_time
                      │
                      ▼
              pitch < DROWSY_RECOVER (10°)?
                      │
                      YES ──▶  dip_duration = now - _nod_enter_time
                                      │
                              >= NOD_SUSTAIN_SEC (0.4 s)?
                                      │
                              YES ────┴──── NO
                               │             │
                          count nod       ignore
                               │         (vibration)
                               ▼
                      DROWSY_NODS nods in DROWSY_WINDOW?
                               │
                               YES ──▶  drowsy_event.set() + buzzer
```

-----

## Thread Map

|Thread      |Rate |Responsibility                                         |
|------------|-----|-------------------------------------------------------|
|`imu`       |50 Hz|Read MPU6050, run physics FSM, feed AI buffer          |
|`ai`        |5 Hz |Run MLP on buffer snapshot, set `_ai_confirmed`        |
|`drowsy`    |10 Hz|Head-nod pitch FSM, drowsiness buzzer control          |
|`demo_watch`|20 Hz|Button hold / triple-press → toggle demo mode          |
|`gps`       |Event|Parse NMEA sentences, cache last fix                   |
|`rider`     |3 s  |Reload `rider.json` (written by BLE companion app)     |
|`oled`      |2 s  |Refresh 128×64 OLED display                            |
|`telemetry` |1 s  |Flush `telemetry.json` for BLE server                  |
|`dashboard` |0.5 s|Redraw ANSI terminal panel (sole stdout writer)        |
|`watchdog`  |2 s  |Heartbeat check + `/dev/watchdog` hardware kick        |
|`main`      |Event|Blocks on `crash_event`; runs `emergency()` exclusively|


> **Thread safety**: `crash_event` is set **only** by `imu_thread`. State transitions are made **only** by `main` thread via `_set_state()`. All shared state uses either GIL-safe scalar writes or explicit locks.

-----

## Getting Started

### Prerequisites

```bash
# System packages
sudo apt update && sudo apt install -y python3-pip python3-smbus i2c-tools

# Python dependencies
pip3 install RPi.GPIO pyserial numpy Pillow \
             adafruit-circuitpython-ssd1306 \
             adafruit-blinka
```

### Enable Required Interfaces

```bash
sudo raspi-config
# Interface Options → I2C        → Enable
# Interface Options → Serial     → Disable shell, Enable hardware serial
# Interface Options → Camera     → (not needed)
```

### Hardware Watchdog (Optional but Recommended)

```bash
echo 'dtoverlay=watchdog,nowayout=0' | sudo tee -a /boot/config.txt
sudo modprobe bcm2835_wdt
```

### Clone and Run

```bash
git clone https://github.com/your-username/AI-Smart-Helmet.git
cd AI-Smart-Helmet

# Create rider profile
cp rider.example.json rider.json
nano rider.json     # fill in name, blood type, emergency contact

# Run (root required for GPIO + watchdog)
sudo python3 smart_helmet.py
```

### Run on Boot (systemd)

```bash
sudo nano /etc/systemd/system/smart-helmet.service
```

```ini
[Unit]
Description=AI Smart Helmet
After=network.target

[Service]
ExecStart=/usr/bin/python3 /home/pi/AI-Smart-Helmet/smart_helmet.py
WorkingDirectory=/home/pi/AI-Smart-Helmet
Restart=always
RestartSec=5
User=root

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl enable smart-helmet
sudo systemctl start smart-helmet
```

-----

## Configuration

All parameters are stored in `config.json` (auto-generated on first run with defaults).

```json
{
  "impact_g":           3.15,
  "freefall_g":         0.40,
  "freefall_ms":        80,

  "demo_impact_g":      1.30,
  "demo_hold_sec":      5.0,

  "ai_crash_prob":      0.82,
  "ai_confirm_window":  2.0,
  "ai_pre_filter_g":    2.50,

  "drowsy_pitch_deg":   25.0,
  "drowsy_nods":        2,
  "drowsy_window_sec":  4.0,
  "nod_sustain_sec":    0.40,

  "cancel_sec":         10,
  "cancel_extra_sec":   5,
  "call_duration":      35,
  "sms_retries":        3,

  "buzzer_pin":         17,
  "button_pin":         18,
  "mpu_addr":           "0x69",
  "gsm_port":           "/dev/ttyS0",
  "gps_port":           "/dev/ttyUSB0",

  "contacts":           ["+91XXXXXXXXXX"],
  "rider_file":         "rider.json",
  "telemetry_file":     "telemetry.json",
  "crash_log":          "crash_log.csv",
  "default_lat":        "17.3422348",
  "default_lon":        "78.3674875"
}
```

|Key                |Default|Description                                              |
|-------------------|-------|---------------------------------------------------------|
|`impact_g`         |`3.15` |Minimum G-force to qualify as crash impact               |
|`freefall_g`       |`0.40` |G below which freefall phase begins                      |
|`freefall_ms`      |`80`   |Freefall must be sustained this long before impact counts|
|`demo_impact_g`    |`1.30` |Impact threshold in demo mode (easy trigger)             |
|`nod_sustain_sec`  |`0.40` |Minimum head-dip duration to count as a real nod         |
|`ai_confirm_window`|`2.0`  |Seconds the AI has to confirm after physics fires        |
|`cancel_sec`       |`10`   |Cancel window before SMS is sent                         |
|`cancel_extra_sec` |`5`    |Extra cancel time added when AI does not confirm         |

### Rider Profile (`rider.json`)

```json
{
  "name":        "Rider Name",
  "blood":       "B+",
  "contact":     "Guardian Name / +91XXXXXXXXXX",
  "allergies":   "None",
  "medical":     "None",
  "belongings":  "Black backpack, blue helmet"
}
```

This file is written by the companion Flutter app over BLE and reloaded by the system every 3 seconds.

-----

## Demo Mode

Demo mode is designed for presentations and testing. It intentionally relaxes crash detection so a light tap on the helmet triggers the full emergency flow.

|Behaviour    |Normal Mode                          |Demo Mode               |
|-------------|-------------------------------------|------------------------|
|Crash trigger|Freefall (≥ 80 ms) + impact (≥ 3.15g)|Any G ≥ 1.30g           |
|SMS tag      |`CRASH ALERT`                        |`CRASH ALERT [DEMO]`    |
|Crash log tag|`N`                                  |`Y`                     |
|Cancel window|Same                                 |Same                    |
|Dashboard    |Normal colours                       |`[DEMO]` badge in yellow|

**Toggle gestures (button on GPIO 18):**

- Hold for ≥ 5 seconds → toggle
- Triple-press within 2 seconds → toggle

-----

## Project Structure

```
AI-Smart-Helmet/
│
├── smart_helmet.py         # Main application (all threads, FSM, detection)
├── config.json             # Runtime configuration (auto-generated)
├── rider.json              # Rider profile (name, blood type, contacts)
│
├── helmet.log              # Rotating application log
├── crash_log.csv           # Persistent crash event history
├── telemetry.json          # Live telemetry (read by Flutter BLE app)
│
├── rider.example.json      # Template for rider profile setup
├── requirements.txt        # Python dependencies
│
└── README.md
```

-----

## Emergency SMS Format

```
CRASH ALERT [AI-confirmed]
Time:     22-04-2026 03:45:10 PM
G-Force:  4.87 g  |  Speed: 62.3 km/h

Name:     Mohd Rehan Rahemath
Blood:    O+
Contact:  Father / +91XXXXXXXXXX
Allergy:  None
Medical:  None

https://maps.google.com/?q=17.342234,78.367487
```

-----

## Changelog

### v5.1 (Current)

- **Demo mode**: completely separated from normal mode FSM — simple threshold trigger, no freefall required
- **False crash fix**: 3-sample moving-average smoothing on G magnitude; road noise and pothole spikes no longer satisfy freefall + impact condition
- **False crash fix**: physics FSM cooldown extended from 1 s to full cancel window (~17 s) to prevent re-triggering mid-emergency
- **Drowsiness fix**: sustain filter (`nod_sustain_sec`) rejects sub-150 ms vibration spikes; only genuine head nods (≥ 0.4 s) are counted
- **SMS deduplication**: lock-guarded `_sms_dispatched` flag ensures exactly one SMS per crash event
- **Config**: `nod_sustain_sec` added as tunable parameter

### v5.0

- Strict physics-only crash trigger (AI confirmation layer, not primary)
- AI extended cancel window when unconfirmed
- Hardware watchdog with 8 s timeout
- Drowsiness head-nod FSM
- Full ANSI dashboard terminal UI
- BLE telemetry JSON output
-----

## License

This project is licensed under the MIT License. See <LICENSE> for details.

-----

<div align="center">

*Built with care for rider safety — Lords Institute of Engineering and Technology · 2025–2026*

</div>
