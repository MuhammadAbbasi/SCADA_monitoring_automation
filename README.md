# SCADA Monitoring Automation

This project automates the extraction of key performance metrics from the VCOM (Meteocontrol) Evaluation dashboard. Built using Python and Playwright, it logs into the web portal, navigates to the designated inverter charts, and seamlessly scrapes structured data.

## Features

- **Automated Authentication**: Securely logs into VCOM using credentials stored outside of version control.
- **Multi-Metric Extraction**: Cycles through and extracts data for:
  - Performance Ratio (PR)
  - AC Power (Potenza AC)
  - Insulation Resistance
  - Inverter Temperature
- **Intelligent Filtering**: Automatically detects and handles inconsistent data columns (e.g., ignoring anomalous "SunGrow SG350HX" entries to prevent data corruption).
- **Continuous Logging**: Operates on a continuous 10-minute cycle, appending new datasets to daily Excel reports without overwriting historical data.

## Future Architecture (MQTT Pipeline)

The current script serves as the foundational data collection agent. In the future, this pipeline will be extended to support real-time mobile notifications via MQTT:

1. **Scraping Layer**: This Playwright script continuously fetches real-time inverter data.
2. **Analysis Layer (Planned)**: The data will be evaluated against dynamic thresholds (e.g., detecting morning inverter shutoffs or PR drops below 5%).
3. **Transport Layer (Planned)**: Triggered alerts will be packaged as JSON payloads and published to an MQTT Broker.
4. **Client Layer (Planned)**: A mobile MQTT client will subscribe to the broker and deliver push notifications directly to a smartphone, enabling instant awareness of plant anomalies.

<details>
<summary>Proposed Stack</summary>

- **Extraction**: Python (Playwright, Pandas)
- **Messaging**: Eclipse Mosquitto / HiveMQ (MQTT)
- **Notification**: MQTT Dash / Pushsafer
</details>

## Installation & Usage

1. **Install Dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Configuration:**
   Create a `config.json` file in the root directory mirroring the following structure:
   ```json
   {
     "USERNAME": "your_vcom_username",
     "PASSWORD": "your_vcom_password",
     "SYSTEM_URL": "https://vcom.meteocontrol.com/vcom/evaluation/..."
   }
   ```

3. **Run the Monitor:**
   ```bash
   python vcom_monitor.py
   ```
   *The script will open a Chromium browser, login, and begin its 10-minute data extraction loop.*
