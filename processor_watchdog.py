import pandas as pd
import numpy as np
import time
import json
import glob
import os
import logging
from datetime import datetime
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# --- SETUP LOGGING ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("analysis.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --- CORE PROCESSING LOGIC ---

def clean_data(df):
    """Ensure data is numeric and handle common extraction artifacts."""
    if df is None or df.empty:
        return pd.DataFrame()
    
    if 'Timestamp Fetch' in df.columns:
        df = df.drop(columns=['Timestamp Fetch'])
        
    for col in df.columns:
        if col == 'Ora' or col == 'DateTime':
            continue
        if df[col].dtype == object:
            # Handle potential string conversions if not already done in extraction
            val = df[col].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
            df[col] = pd.to_numeric(val, errors='coerce')
    return df

def to_hours(t):
    """Convert HH:MM or HH:MM:SS to float hours."""
    if isinstance(t, (int, float)):
        return float(t)
    try:
        parts = str(t).split(':')
        if len(parts) >= 2:
            return int(parts[0]) + int(parts[1])/60
        return np.nan
    except Exception:
        return np.nan

def analyze_site(directory):
    logger.info("Starting forensic analysis of plant data...")
    
    # 1. Load latest files
    file_patterns = {
        'Potenza_AC': "*Potenza_AC_*.xlsx",
        'PR': "*PR_*.xlsx",
        'Resistenza_Isolamento': "*Resistenza_Isolamento_*.xlsx",
        'Temperatura': "*Temperatura_*.xlsx",
        'Corrente_DC': "*Corrente_DC_*.xlsx",
        'Irraggiamento': "*Irraggiamento_*.xlsx"
    }

    file_paths = {}
    for key, pattern in file_patterns.items():
        matches = glob.glob(os.path.join(directory, pattern))
        if matches:
            file_paths[key] = max(matches, key=os.path.getmtime)

    if len(file_paths) < 6:
        logger.warning(f"Missing files for complete analysis. Found {len(file_paths)}/6. Skipping cycle.")
        return

    # 2. Read and Clean
    try:
        pot_df = clean_data(pd.read_excel(file_paths['Potenza_AC']))
        res_df = clean_data(pd.read_excel(file_paths['Resistenza_Isolamento']))
        temp_df = clean_data(pd.read_excel(file_paths['Temperatura']))
        pr_df = clean_data(pd.read_excel(file_paths['PR']))
        dc_df = clean_data(pd.read_excel(file_paths['Corrente_DC']))
        irr_df = clean_data(pd.read_excel(file_paths['Irraggiamento']))
    except Exception as e:
        logger.error(f"Error reading Excel files: {e}")
        return

    # 3. Standardize Time and Merge
    for df in [pot_df, res_df, temp_df, dc_df, irr_df]:
        if not df.empty and 'Ora' in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df['Ora']):
                df['Ora'] = df['Ora'].dt.strftime('%H:%M')
            df['Ora_Numeric'] = df['Ora'].apply(to_hours)

    # Master merge on 'Ora'
    merged = pot_df[['Ora', 'Ora_Numeric']].copy()
    
    # Identify Inverters (assuming columns like 'INV TX1-01', etc.)
    inv_ids = [c for c in pot_df.columns if 'INV' in c and 'Ora' not in c]
    
    # Merge all datasets
    suffixes = {'POT': pot_df, 'RES': res_df, 'TEMP': temp_df, 'DC': dc_df}
    for suffix, df in suffixes.items():
        cols_to_merge = [c for c in df.columns if 'INV' in c] + ['Ora']
        merged = merged.merge(df[cols_to_merge], on='Ora', how='left', suffixes=('', f'_{suffix}'))
    
    # Handle Irradiance separately (sensor names are specific)
    irr_cols = [c for c in irr_df.columns if c != 'Ora' and c != 'Ora_Numeric']
    merged = merged.merge(irr_df[irr_cols + ['Ora']], on='Ora', how='left')

    # Core Analysis Logic
    dashboard_path = os.path.join(directory, 'dashboard_data.json')
    historical_alarms = []
    if os.path.exists(dashboard_path):
        try:
            with open(dashboard_path, 'r') as f:
                historical_alarms = json.load(f).get('historical_alarms', [])
        except Exception: pass

    dashboard_data = {
        "last_updated": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "macro_health": {
            "total_inverters": len(inv_ids),
            "online": 0,
            "tripped": 0,
            "comms_lost": 0
        },
        "anomalies": [],
        "historical_alarms": historical_alarms
    }

    # Reference metrics for Site
    site_production = merged[[c for c in merged.columns if '_POT' in c]].mean(axis=1)
    # Use JB1_POA-1 as primary reference for Irradiance
    poa_ref = merged.get('JB1_POA-1', merged.get('JB3_POA-3', site_production * 0.05)) 

    now_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    for inv in inv_ids:
        pot = merged[f"{inv}_POT"] if f"{inv}_POT" in merged.columns else merged[inv]
        res = merged[f"{inv}_RES"] if f"{inv}_RES" in merged.columns else pd.Series([np.nan]*len(merged))
        temp = merged[f"{inv}_TEMP"] if f"{inv}_TEMP" in merged.columns else pd.Series([np.nan]*len(merged))
        
        # 1. Comms Loss Check (> 20 mins of NaN during production hours)
        daylight = merged[(merged['Ora_Numeric'] >= 7.0) & (merged['Ora_Numeric'] <= 18.5)]
        nan_streak = pot.isnull().rolling(window=4).sum().max() # 4 slots * 5min approx
        if nan_streak >= 4:
            dashboard_data["macro_health"]["comms_lost"] += 1
            alarm = {"timestamp": now_ts, "inverter": inv, "type": "Communication Problem", "severity": "Medium", "details": "No data received for > 20 minutes."}
            dashboard_data["anomalies"].append(alarm)
            continue

        # 2. Trip / 0W Check
        latest_pot = pot.iloc[-1]
        if latest_pot == 0 and site_production.iloc[-1] > 1000:
            dashboard_data["macro_health"]["tripped"] += 1
            
            # Check reason
            last_res = res.dropna().iloc[-1] if not res.dropna().empty else 1000
            last_temp = temp.dropna().iloc[-1] if not temp.dropna().empty else 40
            
            reason = "Unknown Trip"
            if last_res < 50: reason = f"Insulation Fault ({last_res} kOhm)"
            elif last_temp > 60: reason = f"Thermal Trip ({last_temp} °C)"
            
            alarm = {"timestamp": now_ts, "inverter": inv, "type": "Inverter Trip", "severity": "Critical", "details": reason}
            dashboard_data["anomalies"].append(alarm)
            continue
        
        dashboard_data["macro_health"]["online"] += 1

        # 3. Thermal Anomalies
        if temp.max() > 60:
            avg_irr = poa_ref.iloc[-5:].mean()
            if avg_irr > 600: # High production environment
                alarm = {"timestamp": now_ts, "inverter": inv, "type": "Thermal Derating", "severity": "Info", "details": "Normal behavior under high irradiance."}
            else:
                alarm = {"timestamp": now_ts, "inverter": inv, "type": "Thermal Problem", "severity": "High", "details": "High temp despite low irradiance. Possible Fan Failure."}
            dashboard_data["anomalies"].append(alarm)

        # 4. Insulation Resistance (Time-gated)
        if res.iloc[-1] < 50:
            current_hour = datetime.now().hour
            if current_hour >= 9:
                alarm = {"timestamp": now_ts, "inverter": inv, "type": "Low Insulation Resistance", "severity": "High", "details": f"Persistent low resistance: {res.iloc[-1]} kOhm"}
                dashboard_data["anomalies"].append(alarm)

        # 5. DC String Comparison
        # (This would require more granular DC entry data if extracted)
        # For now, we flag if DC Current is disproportionately low compared to AC Power site-wide
        if (pot.iloc[-1] < site_production.iloc[-1] * 0.7) and (poa_ref.iloc[-1] > 200):
            alarm = {"timestamp": now_ts, "inverter": inv, "type": "Low Power / String Fault", "severity": "Medium", "details": "Output significantly below site average."}
            dashboard_data["anomalies"].append(alarm)

    # Sync historical alarms
    dashboard_data["historical_alarms"].extend(dashboard_data["anomalies"])
    dashboard_data["historical_alarms"] = dashboard_data["historical_alarms"][-200:] # Keep last 200

    # Save to JSON
    with open(dashboard_path, 'w') as f:
        json.dump(dashboard_data, f, indent=4)
    logger.info("Forensic analysis complete. Dashboard JSON updated.")

class VCOMHandler(FileSystemEventHandler):
    def __init__(self, directory):
        self.directory = directory
        self.last_run = 0

    def on_modified(self, event):
        if event.src_path.endswith('.xlsx'):
            # Debounce: only run if 30 seconds passed since last trigger
            if time.time() - self.last_run > 30:
                self.last_run = time.time()
                time.sleep(5) # Wait for file lock release
                analyze_site(self.directory)

if __name__ == "__main__":
    target_dir = "./extracted_data"
    os.makedirs(target_dir, exist_ok=True)
    
    logger.info(f"Watchdog active on {target_dir}")
    event_handler = VCOMHandler(target_dir)
    observer = Observer()
    observer.schedule(event_handler, target_dir, recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
