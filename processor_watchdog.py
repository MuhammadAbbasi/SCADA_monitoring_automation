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
    
    # We now include Irraggiamento to verify if 0W is a fault or just nighttime/clouds
    file_patterns = {
        'Potenza_AC': f"*Potenza_AC_*.csv", # Adjusted for testing, replace with date_str in production
        'PR': f"*PR_*.csv",
        'Resistenza_Isolamento': f"*Resistenza_Isolamento_*.csv",
        'Temperatura': f"*Temperatura_*.csv",
        'Irraggiamento': f"*Irraggiamento_*.csv"
    }

    file_paths = {}
    for key, pattern in file_patterns.items():
        matches = glob.glob(os.path.join(directory, pattern))
        if matches:
            file_paths[key] = max(matches, key=os.path.getctime) # Get newest file

    if len(file_paths) < 4:
        print("[Error] Missing required files. Waiting for next extraction...")
        return

    # Load & Clean Data
    pot_df = clean_italian_localization(pd.read_csv(file_paths['Potenza_AC']))
    res_df = clean_italian_localization(pd.read_csv(file_paths['Resistenza_Isolamento']))
    temp_df = clean_italian_localization(pd.read_csv(file_paths['Temperatura']))
    irr_df = clean_italian_localization(pd.read_csv(file_paths.get('Irraggiamento', ''))) if 'Irraggiamento' in file_paths else pd.DataFrame()

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
        "anomalies": []
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
            dashboard_data["anomalies"].append({
                "inverter": inv, "type": "Comms Loss", "severity": "Medium",
                "details": "Missing data during daylight hours."
            })
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
            
            dashboard_data["anomalies"].append({
                "inverter": inv, "type": "0W Trip", "severity": "Critical", "time": str(t_time),
                "details": reason
            })

        # 3. Thermal Derating
        elif (temp.max() > 60) and (pot.mean() < site_avg.mean() * 0.9):
            dashboard_data["anomalies"].append({
                "inverter": inv, "type": "Thermal Derating", "severity": "High",
                "details": f"High Temp ({temp.max()}°C) causing power throttling."
            })

    # Save structured JSON for the frontend
    output_path = os.path.join(directory, 'dashboard_data.json')
    with open(output_path, 'w') as f:
        json.dump(dashboard_data, f, indent=4)
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Analysis Complete. JSON updated.")


# --- WATCHDOG AUTOMATION ---

class VCOMFileHandler(FileSystemEventHandler):
    def __init__(self, directory):
        self.directory = directory
        self.last_trigger = 0
        
    def on_modified(self, event):
        # Prevent double-triggering
        if time.time() - self.last_trigger > 10 and event.src_path.endswith('.csv'):
            print(f"\n[Watchdog] Detected change in {os.path.basename(event.src_path)}")
            time.sleep(3) # Wait for file to finish downloading/writing
            analyze_inverter_data(self.directory)
            self.last_trigger = time.time()

if __name__ == "__main__":
    target_dir = "./extracted_data"  # Ensure this folder exists
    os.makedirs(target_dir, exist_ok=True)
    
    print(f"Starting Watchdog... monitoring folder: {target_dir}")
    event_handler = VCOMFileHandler(target_dir)
    observer = Observer()
    observer.schedule(event_handler, target_dir, recursive=False)
    observer.start()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()