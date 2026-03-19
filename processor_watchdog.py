import pandas as pd
import numpy as np
import time
import json
import glob
import os
from datetime import datetime
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# --- CORE PROCESSING LOGIC ---

def clean_italian_localization(df):
    if 'Timestamp Fetch' in df.columns:
        df = df.drop(columns=['Timestamp Fetch'])
    for col in df.columns:
        if col == 'Ora' or col == 'DateTime': continue
        if df[col].dtype == object:
            val = df[col].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
            df[col] = pd.to_numeric(val, errors='coerce')
    return df

def analyze_inverter_data(directory):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Starting Local Analysis...")
    date_str = datetime.now().strftime("%Y-%m-%d")

    # Retain historical alarm trail across analyses
    dashboard_path = os.path.join(directory, 'dashboard_data.json')
    historical_alarms = []
    try:
        if os.path.exists(dashboard_path):
            with open(dashboard_path, 'r') as f:
                existing = json.load(f)
                historical_alarms = existing.get('historical_alarms', []) or []
    except Exception as e:
        print(f"[Warning] Could not read existing dashboard JSON for history: {e}")

    # We now include Corrente_DC and Irraggiamento to verify additional metrics
    file_patterns = {
        'Potenza_AC': f"*Potenza_AC_*.xlsx",
        'PR': f"*PR_*.xlsx",
        'Resistenza_Isolamento': f"*Resistenza_Isolamento_*.xlsx",
        'Temperatura': f"*Temperatura_*.xlsx",
        'Corrente_DC': f"*Corrente_DC_*.xlsx",
        'Irraggiamento': f"*Irraggiamento_*.xlsx"
    }

    file_paths = {}
    for key, pattern in file_patterns.items():
        matches = glob.glob(os.path.join(directory, pattern))
        if matches:
            file_paths[key] = max(matches, key=os.path.getctime) # Get newest file

    # Require all 6 data types now (including Irraggiamento)
    if len(file_paths) < 6:
        print("[Error] Missing required files. Waiting for next extraction...")
        return

    # Load & Clean Data
    pot_df = clean_italian_localization(pd.read_excel(file_paths['Potenza_AC']))
    res_df = clean_italian_localization(pd.read_excel(file_paths['Resistenza_Isolamento']))
    temp_df = clean_italian_localization(pd.read_excel(file_paths['Temperatura']))
    pr_df = clean_italian_localization(pd.read_excel(file_paths['PR']))
    corrente_df = clean_italian_localization(pd.read_excel(file_paths['Corrente_DC']))
    irr_df = clean_italian_localization(pd.read_excel(file_paths['Irraggiamento']))

    # Standardize Time
    for df in [pot_df, res_df, temp_df]:
        if pd.api.types.is_datetime64_any_dtype(df['Ora']):
            df['Ora'] = df['Ora'].dt.strftime('%H:%M')

    # Merge Data
    merged_ts = pot_df.merge(res_df, on='Ora', suffixes=('_POT', '_RES'))
    merged_ts = merged_ts.merge(temp_df, on='Ora', suffixes=('', '_TEMP'))
    
    # Rename Temp columns safely
    temp_cols = [c for c in temp_df.columns if 'INV' in c and c != 'Ora']
    for c in temp_cols:
        if c in merged_ts.columns and c + '_TEMP' not in merged_ts.columns:
            merged_ts = merged_ts.rename(columns={c: c + '_TEMP'})

    inv_ids = [c.replace('_POT', '') for c in merged_ts.columns if '_POT' in c]
    
    def to_hours(t):
        try: return float(t) if isinstance(t, (int, float)) else int(t.split(':')[0]) + int(t.split(':')[1])/60
        except: return np.nan

    merged_ts['Ora_Numeric'] = merged_ts['Ora'].apply(to_hours)
    daylight_mask = (merged_ts['Ora_Numeric'] >= 6.5) & (merged_ts['Ora_Numeric'] <= 18.0)
    df_day = merged_ts[daylight_mask].copy()
    
    # Baseline calculations
    pot_cols = [id + '_POT' for id in inv_ids]
    df_day['Site_Avg_POT'] = df_day[pot_cols].mean(axis=1)

    # --- FORENSIC ANALYSIS ---
    dashboard_data = {
        "last_updated": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "macro_health": {
            "total_inverters": len(inv_ids),
            "online": len(inv_ids),
            "tripped": 0,
            "comms_lost": 0
        },
        "anomalies": [],
        "historical_alarms": historical_alarms
    }

    for inv in inv_ids:
        pot = df_day[inv + '_POT']
        res = df_day[inv + '_RES']
        temp = df_day[inv + '_TEMP']
        site_avg = df_day['Site_Avg_POT']
        
        # 1. Comms Loss
        if pot.isnull().sum() > 5:
            dashboard_data["macro_health"]["comms_lost"] += 1
            dashboard_data["macro_health"]["online"] -= 1
            alarm = {
                "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "inverter": inv,
                "type": "Comms Loss",
                "severity": "Medium",
                "details": "Missing data during daylight hours."
            }
            dashboard_data["anomalies"].append(alarm)
            dashboard_data["historical_alarms"].append(alarm)
            continue

        # 2. Trip Analysis (0W while site is producing)
        trip_points = df_day[(pot == 0) & (site_avg > 1000)]
        if not trip_points.empty:
            dashboard_data["macro_health"]["tripped"] += 1
            dashboard_data["macro_health"]["online"] -= 1
            
            t_time = trip_points.iloc[0]['Ora']
            t_num = trip_points.iloc[0]['Ora_Numeric']
            
            # Check 60 mins prior
            pre_trip = merged_ts[(merged_ts['Ora_Numeric'] < t_num) & (merged_ts['Ora_Numeric'] >= t_num - 1.0)]
            min_res = pre_trip[inv + '_RES'].min() if not pre_trip.empty else "N/A"
            max_temp = pre_trip[inv + '_TEMP'].max() if not pre_trip.empty else "N/A"
            
            reason = "Unknown Trip"
            if min_res != "N/A" and min_res < 50: reason = f"Insulation Fault ({min_res} kOhm)"
            elif max_temp != "N/A" and max_temp > 60: reason = f"Thermal Trip ({max_temp} °C)"
            
            alarm = {
                "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "inverter": inv,
                "type": "0W Trip",
                "severity": "Critical",
                "time": str(t_time),
                "details": reason
            }
            dashboard_data["anomalies"].append(alarm)
            dashboard_data["historical_alarms"].append(alarm)

        # 3. Thermal Derating
        elif (temp.max() > 60) and (pot.mean() < site_avg.mean() * 0.9):
            alarm = {
                "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "inverter": inv,
                "type": "Thermal Derating",
                "severity": "High",
                "details": f"High Temp ({temp.max()}°C) causing power throttling."
            }
            dashboard_data["anomalies"].append(alarm)
            dashboard_data["historical_alarms"].append(alarm)

    # Cap the stored history to prevent unbounded growth
    history_limit = 200
    dashboard_data["historical_alarms"] = dashboard_data["historical_alarms"][-history_limit:]

    # Save structured JSON for the frontend
    output_path = os.path.join(directory, 'dashboard_data.json')
    with open(output_path, 'w') as f:
        json.dump(dashboard_data, f, indent=4)
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Analysis Complete. JSON updated.")


# --- WATCHDOG AUTOMATION ---

class VCOMFileHandler(FileSystemEventHandler):
    def __init__(self, directory):
        self.directory = directory
        self.target_keywords = ['Potenza_AC', 'PR', 'Resistenza_Isolamento', 'Temperatura', 'Corrente_DC', 'Irraggiamento']
        self.last_analysis_time = 0
        
    def wait_for_file_stability(self, filepath, timeout=30):
        """ Waits for file size to stop changing, ensuring write is complete. """
        start_time = time.time()
        last_size = -1
        
        while time.time() - start_time < timeout:
            try:
                current_size = os.path.getsize(filepath)
                if current_size == last_size and current_size > 0:
                    # Size stabilized
                    time.sleep(2) # Final buffer
                    return True
                last_size = current_size
            except OSError:
                # File might be locked or not yet available
                pass
            time.sleep(3)
        
        print(f"[Watchdog] Timeout waiting for file stability: {filepath}")
        return False
        
    def get_latest_dataset_mod_time(self):
        """Return max modification time (epoch) across all required sheets.

        Returns 0 if not all required files are present.
        """
        max_mod = 0
        for kw in self.target_keywords:
            matches = glob.glob(os.path.join(self.directory, f"*{kw}_*.xlsx"))
            if not matches:
                return 0
            latest_file = max(matches, key=os.path.getmtime)
            max_mod = max(max_mod, os.path.getmtime(latest_file))
        return max_mod

    def handle_event(self, event):
        # Ignore directory changes
        if event.is_directory:
            return

        # We only care about .xlsx files
        if not event.src_path.endswith('.xlsx'):
            return

        filename = os.path.basename(event.src_path)
        print(f"[Watchdog] Detected file event: {filename}")

        # Wait for the file to finish writing
        if not self.wait_for_file_stability(event.src_path):
            return

        latest_mod = self.get_latest_dataset_mod_time()
        if latest_mod == 0:
            print("[Watchdog] Waiting for all required data files to be present...")
            return

        if latest_mod > self.last_analysis_time:
            print(f"[Watchdog] New data detected (mod_time={latest_mod}). Running analysis...")
            try:
                analyze_inverter_data(self.directory)
                self.last_analysis_time = latest_mod
            except Exception as e:
                print(f"[Watchdog] Error during analysis: {e}")
        else:
            print("[Watchdog] No new data since last analysis.")

    # Listen for when a file is modified/updated
    def on_modified(self, event):
        self.handle_event(event)

    # Listen for when a new file is created/dropped into the folder
    def on_created(self, event):
        self.handle_event(event)
        
    # Listen for when a temp file is renamed to the final .xlsx
    def on_moved(self, event):
        # For 'moved' events, the new file name is in dest_path
        class MockEvent:
            is_directory = event.is_directory
            src_path = event.dest_path
        self.handle_event(MockEvent())

if __name__ == "__main__":
    target_dir = "./extracted_data"  # Ensure this folder exists
    os.makedirs(target_dir, exist_ok=True)
    
    print(f"Starting Watchdog... monitoring folder: {target_dir}")
    print("Waiting for all 6 data files: Potenza_AC, PR, Resistenza_Isolamento, Temperatura, Corrente_DC, Irraggiamento")
    event_handler = VCOMFileHandler(target_dir)
    observer = Observer()
    observer.schedule(event_handler, target_dir, recursive=False)
    observer.start()
    
    try:
        # Periodic safety check: ensure analysis reruns at least every 10 minutes if files update
        next_check = time.time() + 600  # 10 minutes
        while True:
            time.sleep(1)

            if time.time() >= next_check:
                next_check = time.time() + 600
                latest_mod = event_handler.get_latest_dataset_mod_time()
                if latest_mod > event_handler.last_analysis_time:
                    print("[Watchdog] Periodic check: new data detected, running analysis...")
                    try:
                        analyze_inverter_data(target_dir)
                        event_handler.last_analysis_time = latest_mod
                    except Exception as e:
                        print(f"[Watchdog] Error during periodic analysis: {e}")
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
