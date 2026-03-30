# Mazara Monitoring System

Production-ready automated SCADA monitoring system for a solar plant (3 transformers, 36 inverters).

## System Architecture

1.  **Data Extraction**: Playwright-based scraper (`extraction_code/vcom_monitor.py`) fetches real-time metrics (PR, Power, Temp, Insulation, etc.) every 10 minutes from VCOM.
2.  **ETL & Storage**: Cleaned data is appended to daily Excel files in `extracted_data/`.
3.  **Analytics Engine**: `processor_watchdog.py` monitors data files and performs forensic analysis (Trips, Comms Loss, Thermal Derating, etc.).
4.  **Dashboard**: FastAPI-based REST API and premium dark-mode frontend serving real-time health status on port 8080.
5.  **DevOps**: Dockerized deployment using `supervisord` to manage all services.

## Installation

### Local Execution
1.  Install dependencies: `pip install -r requirements.txt`
2.  Configure `config.json` with your VCOM credentials.
3.  Install Playwright browsers: `playwright install chromium`
4.  Run all services (Extraction, Watchdog, Dashboard).

### Docker Deployment
1.  Build image: `docker build -t mazara-monitor .`
2.  Run container: `docker run -p 8080:8080 mazara-monitor`

## Dashboard
The dashboard is accessible at `http://localhost:8080` (or the server's IP address on the local network). It provides:
- At-a-glance plant health overview.
- Active diagnostic alerts with severity levels.
- Historical alarm trail for long-term troubleshooting.

## Components & Sensors
- **Inverters**: 36 units (TX1-01 to TX3-12).
- **Environment**: 14 specific irradiance and environmental sensors.
