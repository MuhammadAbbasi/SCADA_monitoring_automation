import logging
import time
import os
import json
from datetime import datetime
import pandas as pd
from playwright.sync_api import sync_playwright

from pr_monitor import extract_pr_data
from potenza_ac_monitor import extract_potenza_ac_data
from insulation_resistance_monitor import extract_insulation_resistance_data
from temperature_monitor import extract_temperature_data
from corrente_dc_monitor import extract_corrente_dc_data
from irradiance_monitor import extract_irradiance_data

# --- SETUP LOGGING ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("monitoring.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Paths relative to this script's location
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_ROOT_DIR = os.path.dirname(_SCRIPT_DIR)           # Automation/
_EXTRACTED_DATA_DIR = os.path.join(_ROOT_DIR, "extracted_data")

# Read configuration from config.json at root
config_path = os.path.join(_ROOT_DIR, "config.json")
try:
    with open(config_path, "r") as f:
        config_data = json.load(f)
except Exception as e:
    logger.error(f"Failed to load config.json: {e}")
    raise

USERNAME = config_data.get("USERNAME")
PASSWORD = config_data.get("PASSWORD")
SYSTEM_URL = config_data.get("SYSTEM_URL")

# Inverter ID Mapping based on provided HTML
INVERTER_IDS = [
    "Id2784833",   # INV TX1-01
    "Id2784832",   # INV TX1-02
    "Id2784831",   # INV TX1-03
    "Id27848312",  # INV TX1-04
    "Id2784835",   # INV TX1-05
    "Id2784837",   # INV TX1-06
    "Id27848310",  # INV TX1-07
    "Id2784838",   # INV TX1-08
    "Id2784839",   # INV TX1-09
    "Id2784834",   # INV TX1-10
    "Id2784836",   # INV TX1-11
    "Id27848311",  # INV TX1-12
    "Id27848212",  # INV TX2-01
    "Id27848211",  # INV TX2-02
    "Id2784822",   # INV TX2-03
    "Id2784829",   # INV TX2-04
    "Id27848210",  # INV TX2-05
    "Id2784821",   # INV TX2-06
    "Id2784828",   # INV TX2-07
    "Id2784823",   # INV TX2-08
    "Id2784824",   # INV TX2-09
    "Id2784825",   # INV TX2-10
    "Id2784827",   # INV TX2-11
    "Id2784826",   # INV TX2-12
    "Id2784879",   # INV TX3-01
    "Id2784872",   # INV TX3-02
    "Id2784877",   # INV TX3-03
    "Id2784875",   # INV TX3-04
    "Id2784874",   # INV TX3-05
    "Id2784876",   # INV TX3-06
    "Id2784873",   # INV TX3-07
    "Id27848712",  # INV TX3-08
    "Id27848710",  # INV TX3-09
    "Id2784878",   # INV TX3-10
    "Id2784871",   # INV TX3-11
    "Id27848711"   # INV TX3-12
]

def login(page):
    logger.info("Logging into VCOM meteocontrol...")
    page.goto(SYSTEM_URL, timeout=60000)

    # Wait for login form
    page.wait_for_selector('input[type="text"]')
    page.fill('input[type="text"]', USERNAME)
    page.fill('input[type="password"]', PASSWORD)

    # Click Login
    page.locator('button:has-text("Login"), button[type="submit"]').first.click()

    # Dismiss cookie banner if it appears right after login
    try:
        if page.locator('button:has-text("Usa solo i cookie necessari")').is_visible(timeout=5000):
            page.locator('button:has-text("Usa solo i cookie necessari")').click()
    except Exception:
        pass

    logger.info("Post-login redirect handling...")
    page.wait_for_selector('text="Valutazione"', timeout=60000)
    page.locator('text="Valutazione"').first.click()

    try:
        page.wait_for_selector('text="Inverter"', timeout=30000)
        logger.info("Successfully logged in and reached the Evaluation dashboard.")
    except Exception as e:
        logger.error(f"Failed to reach dashboard or timeout: {e}")
        page.screenshot(path=os.path.join(_ROOT_DIR, "errors", "login_error.png"))

def select_inverters(page):
    """Ensures only the specific 36 inverters are selected, excluding SunGrow."""
    logger.info("Selecting target inverters and excluding SunGrow...")
    try:
        # Open component selection if it's not visible
        # (Usually it is visible in 'Valutazione' under components section)
        
        # Deselect all first to have a clean slate
        if page.locator('button:has-text("Deselect all")').is_visible():
            page.locator('button:has-text("Deselect all")').click()
            time.sleep(1)

        for inv_id in INVERTER_IDS:
            checkbox_id = f"checkbox-{inv_id}"
            cb = page.locator(f"input#{checkbox_id}")
            if cb.is_visible():
                cb.check()
        
        # Explicitly ensure SunGrow is unchecked
        sungrow_cb = page.locator('input[id*="Id27848313"]') # Based on snippet
        if sungrow_cb.is_visible() and sungrow_cb.is_checked():
            sungrow_cb.uncheck()

        # Click Update chart
        if page.locator('button:has-text("Aggiorna grafico"), button:has-text("Update chart")').is_visible():
            page.locator('button:has-text("Aggiorna grafico"), button:has-text("Update chart")').click()
            time.sleep(2)
            
    except Exception as e:
        logger.warning(f"Error during inverter selection: {e}")

def append_df_to_excel(filename, df, sheet_name='Sheet1'):
    """Append a DataFrame to an existing Excel file, or create it if it doesn't exist."""
    if not os.path.exists(filename):
        df.to_excel(filename, index=False, sheet_name=sheet_name)
    else:
        with pd.ExcelWriter(filename, engine='openpyxl', mode='a', if_sheet_exists='overlay') as writer:
            write_header = sheet_name not in writer.sheets
            startrow = writer.sheets[sheet_name].max_row if not write_header else 0
            df.to_excel(writer, sheet_name=sheet_name, startrow=startrow, index=False, header=write_header)

def export_to_excel(df_pr, df_ac, df_insulation, df_temp, df_dc, df_irradiance):
    today_str = datetime.now().strftime("%Y-%m-%d")
    current_time = datetime.now().strftime("%H:%M:%S")

    os.makedirs(_EXTRACTED_DATA_DIR, exist_ok=True)

    files = {
        "PR": os.path.join(_EXTRACTED_DATA_DIR, f"PR_{today_str}.xlsx"),
        "Potenza_AC": os.path.join(_EXTRACTED_DATA_DIR, f"Potenza_AC_{today_str}.xlsx"),
        "Resistenza_Isolamento": os.path.join(_EXTRACTED_DATA_DIR, f"Resistenza_Isolamento_{today_str}.xlsx"),
        "Temperatura": os.path.join(_EXTRACTED_DATA_DIR, f"Temperatura_{today_str}.xlsx"),
        "Corrente_DC": os.path.join(_EXTRACTED_DATA_DIR, f"Corrente_DC_{today_str}.xlsx"),
        "Irraggiamento": os.path.join(_EXTRACTED_DATA_DIR, f"Irraggiamento_{today_str}.xlsx")
    }

    logger.info(f"Exporting data for cycle at {current_time}...")

    data_map = {
        "PR": df_pr,
        "Potenza_AC": df_ac,
        "Resistenza_Isolamento": df_insulation,
        "Temperatura": df_temp,
        "Corrente_DC": df_dc,
        "Irraggiamento": df_irradiance
    }

    count = 0
    for key, df in data_map.items():
        if df is not None and not df.empty:
            if 'Timestamp Fetch' not in df.columns:
                df.insert(0, 'Timestamp Fetch', current_time)
            append_df_to_excel(files[key], df)
            logger.info(f"[OK] Appended: {files[key]}")
            count += 1
    
    logger.info(f"[FINISH] {count}/6 files updated successfully.")

def run_extraction_cycle(page):
    """Runs a single extraction and export cycle with retries."""
    
    # Ensure inverters are correctly selected at the start of cycle
    select_inverters(page)

    metrics = [
        ("PR", extract_pr_data),
        ("Potenza AC", extract_potenza_ac_data),
        ("Insulation", extract_insulation_resistance_data),
        ("Temperature", extract_temperature_data),
        ("Corrente DC", extract_corrente_dc_data),
        ("Irraggiamento", extract_irradiance_data)
    ]

    results = {}
    for name, func in metrics:
        success = False
        for attempt in range(2): # 2 attempts per metric
            try:
                results[name] = func(page)
                success = True
                break
            except Exception as e:
                logger.error(f"Attempt {attempt+1} failed for {name}: {e}")
                page.screenshot(path=os.path.join(_ROOT_DIR, "errors", f"error_{name.replace(' ', '_')}_{attempt+1}.png"))
                time.sleep(5)
        if not success:
            logger.warning(f"Metric {name} failed after all attempts. Moving to next.")
            results[name] = pd.DataFrame()

    export_to_excel(
        results["PR"], 
        results["Potenza AC"], 
        results["Insulation"], 
        results["Temperature"], 
        results["Corrente DC"], 
        results["Irraggiamento"]
    )

def main():
    extraction_interval_minutes = 10
    os.makedirs(os.path.join(_ROOT_DIR, "errors"), exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(viewport={'width': 1450, 'height': 900})
        page = context.new_page()

        try:
            login(page)

            cycle_count = 1
            while True:
                logger.info(f"=== Starting Extraction Cycle #{cycle_count} ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')}) ===")
                
                try:
                    # Check if session is still alive by searching for a dashboard element
                    if not page.locator('text="Valutazione"').is_visible(timeout=5000):
                        logger.warning("Session might have expired. Re-logging in...")
                        login(page)
                    else:
                        page.locator('text="Valutazione"').first.click()
                        time.sleep(2)
                except Exception:
                    logger.warning("Navigation error. Attempting re-login...")
                    login(page)

                run_extraction_cycle(page)
                
                logger.info(f"Cycle #{cycle_count} completed. Waiting {extraction_interval_minutes} minutes...")
                time.sleep(extraction_interval_minutes * 60)
                cycle_count += 1

        except KeyboardInterrupt:
            logger.info("Script interrupted by user. Exiting...")
        except Exception as e:
            logger.critical(f"FATAL ERROR: {e}")
            page.screenshot(path=os.path.join(_ROOT_DIR, "errors", "fatal_error.png"))
        finally:
            logger.info("Closing browser...")
            browser.close()

if __name__ == "__main__":
    main()
