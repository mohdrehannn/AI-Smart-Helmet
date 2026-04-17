# AI-Smart-Helmet
## Smart Helmet with Accident Detection and
Emergency Alert System
Objective: To develop a safety system for riders that can automatically detect accidents and
monitor rider condition, sending emergency alerts and initiating a call when necessary.
Features:
1. Crash Detection using MPU6050 based on G-force.
2. Alert & Cancel System with buzzer and push button.
3. GPS Location Tracking using NEO-7M module with fallback.
4. GSM Emergency SMS using SIM800L with rider details and location.
5. Automatic Emergency Call after SMS alert.
6. Drowsiness Detection using vertical head movement (TinyML logic).
7. Mobile App Integration via Bluetooth for rider data.
8. OLED Display for system status and alerts.
9. Buzzer alerts for crash and drowsiness warnings.
10. Smart filtering logic to reduce false triggers.
Components Used:
Processing Unit: Raspberry Pi Zero 2W
Communication Modules:
- SIM800L GSM Module
- GPS Module (NEO-7M)
- Bluetooth (built-in Raspberry Pi)
Sensors:
- MPU6050 (Accelerometer + Gyroscope)
Display & Output:
- OLED Display (SSD1306 I2C)
- Buzzer
Input:
- Push Button (for alert cancel)
Power System:
- Lithium-ion Battery (primary power source)
- Separate stable power supply for SIM800L (to handle current spikes)
- Voltage regulation for safe operation
Audio & Signal Conditioning (for GSM stability):
- Capacitors for smoothing voltage fluctuations
- Supporting resistors and wiring
Connectivity & Support:
- USB to UART Converter (for GPS)
- USB Hub (for multiple peripherals)
- Connecting wires and PCB/board setup
Summary: The system enhances rider safety by detecting accidents, monitoring drowsiness, and
ensuring emergency communication through SMS and calls.
