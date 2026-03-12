import smbus
import time
import RPi.GPIO as GPIO
import serial
from datetime import datetime

# ---------------- GPIO SETUP ----------------

BUZZER = 17
BUTTON = 18
TOUCH = 24

GPIO.setmode(GPIO.BCM)
GPIO.setup(BUZZER, GPIO.OUT)
GPIO.setup(BUTTON, GPIO.IN, pull_up_down=GPIO.PUD_UP)
GPIO.setup(TOUCH, GPIO.IN)

# ---------------- IMU SETUP ----------------

bus = smbus.SMBus(1)
MPU_ADDR = 0x68
bus.write_byte_data(MPU_ADDR, 0x6B, 0)

# ---------------- SERIAL SETUP ----------------

gsm = serial.Serial("/dev/ttyS0", baudrate=9600, timeout=1)

# ---------------- SETTINGS ----------------

PHONE_NUMBER = "+919494844078"
CRASH_THRESHOLD = 3.15

# fallback demo location
LAT = "17.3422348"
LON = "78.3674875"

# ---------------- FUNCTIONS ----------------

def read_accel():
    data = bus.read_i2c_block_data(MPU_ADDR, 0x3B, 6)

    ax = data[0] << 8 | data[1]
    ay = data[2] << 8 | data[3]
    az = data[4] << 8 | data[5]

    if ax > 32767: ax -= 65536
    if ay > 32767: ay -= 65536
    if az > 32767: az -= 65536

    ax = ax / 16384.0
    ay = ay / 16384.0
    az = az / 16384.0

    g = (ax**2 + ay**2 + az**2) ** 0.5

    return g


def buzzer_pattern():
    for i in range(10):
        GPIO.output(BUZZER, 1)
        time.sleep(0.15)
        GPIO.output(BUZZER, 0)
        time.sleep(0.15)


def send_sms(message):

    gsm.write(b'AT\r')
    time.sleep(1)

    gsm.write(b'AT+CMGF=1\r')
    time.sleep(1)

    cmd = f'AT+CMGS="{PHONE_NUMBER}"\r'
    gsm.write(cmd.encode())

    time.sleep(1)

    gsm.write(message.encode())
    gsm.write(bytes([26]))

    time.sleep(5)


def build_sms():

    now = datetime.now()
    t = now.strftime("%H:%M:%S")

    message = f"""
SMART HELMET ALERT

Crash Detected!

Time: {t}

Location:
https://maps.google.com/?q={LAT},{LON}

Blood Group: B+
Emergency Contact: 9494844078
"""

    return message


# ---------------- MAIN LOOP ----------------

print("SMART HELMET SYSTEM STARTED")

while True:

    helmet_worn = GPIO.input(TOUCH)

    if helmet_worn:

        g = read_accel()

        print(f"G FORCE: {round(g,2)}")

        if g > CRASH_THRESHOLD:

            print("CRASH DETECTED!")

            start = time.time()

            while time.time() - start < 10:

                buzzer_pattern()

                if GPIO.input(BUTTON) == 0:

                    print("ALARM CANCELLED")
                    break

            else:

                print("SENDING EMERGENCY SMS")

                sms = build_sms()
                send_sms(sms)

    else:

        print("Helmet not worn")

    time.sleep(0.5)
