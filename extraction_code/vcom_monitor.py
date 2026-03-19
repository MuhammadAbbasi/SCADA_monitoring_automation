import time
import os
from datetime import datetime
import pandas as pd
from playwright.sync_api import sync_playwright

from pr_monitor import extract_pr_data
from potenza_ac_monitor import extract_potenza_ac_data
from insulation_resistance_monitor import extract_insulation_resistance_data
from temperature_monitor import extract_temperature_data

import json

# Paths relative to this script's location
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_ROOT_DIR = os.path.dirname(_SCRIPT_DIR)           # Automation/
_EXTRACTED_DATA_DIR = os.path.join(_ROOT_DIR, "extracted_data")

# Read configuration from config.json at root
config_path = os.path.join(_ROOT_DIR, "config.json")
with open(config_path, "r") as f:
    config_data = json.load(f)

USERNAME = config_data.get("USERNAME")
PASSWORD = config_data.get("PASSWORD")
SYSTEM_URL = config_data.get("SYSTEM_URL")


def login(page):
    print("Logging into VCOM meteocontrol...")
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
    except:
        pass

    print("Post-login redirect handling...")
    page.wait_for_selector('text="Valutazione"', timeout=60000)
    page.locator('text="Valutazione"').first.click()

    try:
        page.wait_for_selector('text="Inverter"', timeout=30000)
        print("Successfully logged in and reached the Evaluation dashboard.")
    except Exception as e:
        print(f"Failed to reach dashboard or timeout: {e}")
        page.screenshot(path="login_error.png")


def append_df_to_excel(filename, df, sheet_name='Sheet1'):
    """Append a DataFrame to an existing Excel file, or create it if it doesn't exist."""
    if not os.path.exists(filename):
        # Create new file
        df.to_excel(filename, index=False, sheet_name=sheet_name)
    else:
        # Append to existing file
        with pd.ExcelWriter(filename, engine='openpyxl', mode='a', if_sheet_exists='overlay') as writer:
            # Check if sheet exists to decide whether to write header
            write_header = sheet_name not in writer.sheets

            # Find the last row in the existing sheet
            startrow = writer.sheets[sheet_name].max_row if not write_header else 0

            df.to_excel(writer, sheet_name=sheet_name, startrow=startrow, index=False, header=write_header)


def export_to_excel(df_pr, df_ac, df_insulation, df_temp):
    """Append DataFrames to separate Excel files in the extracted_data folder."""
    today_str = datetime.now().strftime("%Y-%m-%d")
    current_time = datetime.now().strftime("%H:%M:%S")

    # Ensure output directory exists
    os.makedirs(_EXTRACTED_DATA_DIR, exist_ok=True)

    # Define separate filenames
    file_pr = os.path.join(_EXTRACTED_DATA_DIR, f"PR_{today_str}.xlsx")
    file_ac = os.path.join(_EXTRACTED_DATA_DIR, f"Potenza_AC_{today_str}.xlsx")
    file_insulation = os.path.join(_EXTRACTED_DATA_DIR, f"Resistenza_Isolamento_{today_str}.xlsx")
    file_temp = os.path.join(_EXTRACTED_DATA_DIR, f"Temperatura_{today_str}.xlsx")

    print(f"\n[{current_time}] Esportazione/Accodamento dei dati in corso...")

    try:
        # Append PR Data
        if not df_pr.empty:
            df_pr.insert(0, 'Timestamp Fetch', current_time)
            append_df_to_excel(file_pr, df_pr)
            print(f"[OK] Accodato: {file_pr}")

        # Append AC Data
        if not df_ac.empty:
            df_ac.insert(0, 'Timestamp Fetch', current_time)
            append_df_to_excel(file_ac, df_ac)
            print(f"[OK] Accodato: {file_ac}")

        # Append Insulation Data
        if not df_insulation.empty:
            df_insulation.insert(0, 'Timestamp Fetch', current_time)
            append_df_to_excel(file_insulation, df_insulation)
            print(f"[OK] Accodato: {file_insulation}")

        # Append Temperature Data
        if not df_temp.empty:
            df_temp.insert(0, 'Timestamp Fetch', current_time)
            append_df_to_excel(file_temp, df_temp)
            print(f"[OK] Accodato: {file_temp}")

        print(f"[FINISH] Tutti i file ({sum([not df.empty for df in [df_pr, df_ac, df_insulation, df_temp]])}) sono stati aggiornati con successo.")

    except Exception as e:
        print(f"[ERROR] Failed to export/append files: {e}")


def run_extraction_cycle(page):
    """Runs a single extraction and export cycle."""
    try:
        # --- PR ---
        df_pr = extract_pr_data(page)
        time.sleep(2)

        # --- Potenza AC ---
        df_ac = extract_potenza_ac_data(page)
        time.sleep(2)

        # --- Insulation Resistance ---
        df_insulation = extract_insulation_resistance_data(page)
        time.sleep(2)

        # --- Temperature ---
        df_temp = extract_temperature_data(page)

        # --- Export ---
        export_to_excel(df_pr, df_ac, df_insulation, df_temp)

    except Exception as e:
        print(f"\n[ERROR] An error occurred during extraction cycle: {e}")
        page.screenshot(path="error_screenshot.png")
        print("Saved error_screenshot.png for debugging.")


def main():
    extraction_interval_minutes = 10

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(viewport={'width': 1280, 'height': 800})
        page = context.new_page()

        try:
            login(page)

            # Dismiss cookie banner if it appears
            try:
                if page.locator('button:has-text("Usa solo i cookie necessari")').is_visible(timeout=2000):
                    page.locator('button:has-text("Usa solo i cookie necessari")').click()
            except:
                pass

            # Loop indefinitely
            cycle_count = 1
            while True:
                print(f"\n=== Avvio Ciclo di Estrazione #{cycle_count} ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')}) ===")

                # Make sure we're on the dashboard
                try:
                    page.locator('text="Valutazione"').first.click()
                    time.sleep(2)
                except:
                    pass

                run_extraction_cycle(page)
                print(f"\nCiclo #{cycle_count} completato. Attesa di {extraction_interval_minutes} minuti per il prossimo ciclo...")
                time.sleep(extraction_interval_minutes * 60)
                cycle_count += 1

        except KeyboardInterrupt:
            print("\n[INFO] Script interrotto dall'utente. Uscita in corso...")
        except Exception as e:
            print(f"\n[FATAL ERROR] {e}")
            page.screenshot(path="fatal_error_screenshot.png")
        finally:
            print("\nClosing browser...")
            browser.close()


if __name__ == "__main__":
    main()
