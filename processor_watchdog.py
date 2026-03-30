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
    
    # 1. Load latest files and track status
    file_patterns = {
        'Potenza_AC': "*Potenza_AC_*.xlsx",
        'PR': "*PR_*.xlsx",
        'Resistenza_Isolamento': "*Resistenza_Isolamento_*.xlsx",
        'Temperatura': "*Temperatura_*.xlsx",
        'Corrente_DC': "*Corrente_DC_*.xlsx",
        'Irraggiamento': "*Irraggiamento_*.xlsx"
    }

    file_paths = {}
    file_statuses = {}
    dataframes = {k: pd.DataFrame() for k in file_patterns.keys()}

    for key, pattern in file_patterns.items():
        matches = glob.glob(os.path.join(directory, pattern))
        if matches:
            file_paths[key] = max(matches, key=os.path.getmtime)
            try:
                dataframes[key] = clean_data(pd.read_excel(file_paths[key]))
                file_statuses[key] = {"status": "success", "timestamp": datetime.fromtimestamp(os.path.getmtime(file_paths[key])).strftime('%H:%M')}
            except Exception as e:
                logger.error(f"Error reading Excel file {key}: {e}")
                file_statuses[key] = {"status": "pending", "data": None}
        else:
            file_statuses[key] = {"status": "pending", "data": None}

    # 3. Standardize Time and Merge (if at least Potenza_AC exists)
    pot_df = dataframes['Potenza_AC']
    if pot_df.empty:
        logger.warning("Potenza_AC missing. Cannot perform full analysis.")
        # Proceed with empty analysis structure
        dashboard_data = {
            "macro_health": {"total_inverters": 0, "online": 0, "tripped": 0, "comms_lost": 0},
            "anomalies": [],
            "file_statuses": file_statuses
        }
    else:
        # Standardize timestamps for all available dataframes
        for k, df in dataframes.items():
            if not df.empty and 'Ora' in df.columns:
                if pd.api.types.is_datetime64_any_dtype(df['Ora']):
                    df['Ora'] = df['Ora'].dt.strftime('%H:%M')
                df['Ora_Numeric'] = df['Ora'].apply(to_hours)

        # Master merge on 'Ora' using Potenza_AC as reference
        merged = pot_df[['Ora', 'Ora_Numeric']].copy()
        inv_ids = [c for c in pot_df.columns if 'INV' in c and 'Ora' not in c]
        
        # Merge available datasets
        suffix_map = {
            'Resistenza_Isolamento': '_RES', 
            'Temperatura': '_TEMP', 
            'Corrente_DC': '_DC',
            'PR': '_PR'
        }
        for key in suffix_map.keys():
            df = dataframes[key]
            if not df.empty:
                cols_to_merge = [c for c in df.columns if 'INV' in c] + ['Ora']
                merged = merged.merge(df[cols_to_merge], on='Ora', how='left', suffixes=('', suffix_map[key]))

        # Handle Irradiance separately
        irr_df = dataframes['Irraggiamento']
        if not irr_df.empty:
            irr_cols = [c for c in irr_df.columns if c != 'Ora' and c != 'Ora_Numeric']
            merged = merged.merge(irr_df[irr_cols + ['Ora']], on='Ora', how='left')

        # --- FULL FORENSIC SCAN (Incident Detective) ---
        all_incidents = []
        
        # Calculate Site-wide Production (Median is more robust to outliers)
        active_production = merged[[c for c in merged.columns if (c in inv_ids or '_POT' in c)]]
        site_production = active_production.median(axis=1) if not pot_df.empty else pd.Series([0]*len(merged))
        
        today_str = datetime.now().strftime('%Y-%m-%d')
        total_invs = len(inv_ids)

        for idx, inv in enumerate(inv_ids):
            logger.info(f"Scanning Inverter {idx+1}/{total_invs}: {inv}...")
            pot = merged[inv] if inv in merged.columns else merged.get(f"{inv}_POT", pd.Series([np.nan]*len(merged)))
            res = merged.get(f"{inv}_RES", pd.Series([np.nan]*len(merged)))
            temp = merged.get(f"{inv}_TEMP", pd.Series([np.nan]*len(merged)))
            pr_data = merged.get(f"{inv}_PR", pd.Series([np.nan]*len(merged)))
            
            # Pre-calculate DC columns for this inverter
            dc_cols = [c for c in merged.columns if (inv in c and '_DC' in c)]
            # If we have DC strings, get their values as a matrix for fast access
            dc_matrix = merged[dc_cols].values if dc_cols else np.array([[]]*len(merged))
            
            # Detect Events throughout the entire dataframe
            # Using zip avoids the heavy overhead of iloc[i] in a tight loop
            for i, (row_time, row_h, p_val, r_val, t_val, pr_val, s_prod, dc_vals) in enumerate(zip(
                merged['Ora'], merged['Ora_Numeric'], pot, res, temp, pr_data, site_production, dc_matrix
            )):
                
                incident = None

                # Rule 1: Performance Ratio < 85% (Error)
                if not pd.isna(pr_val) and (pr_val < 0.85 or (pr_val < 85 and pr_val > 1)) and row_h >= 9 and row_h <= 17:
                    display_pr = pr_val if pr_val < 100 else pr_val/100
                    incident = {"type": "Low Performance Ratio", "severity": "Critical", "details": f"PR: {round(display_pr, 2)}%"}

                # Rule 2: Inverter Temperature > 40°C (Warning)
                elif not pd.isna(t_val) and t_val > 40:
                    incident = {"type": "High Operating Temp", "severity": "Warning", "details": f"Measured: {t_val}°C"}

                # Rule 3: Low DC current in any string (Error)
                elif dc_cols and p_val > 500: # only check if inverter is producing
                    for col_idx, dcv in enumerate(dc_vals):
                        if not pd.isna(dcv) and dcv < 0.2: # Simple threshold for "low" DC current
                            incident = {"type": "DC String Failure", "severity": "Critical", "details": f"Low current on {dc_cols[col_idx]}"}
                            break

                # Rule 4: AC Power difference > 3% (Error)
                if not incident and s_prod > 5000: # Site is producing significantly
                    p_diff = abs(p_val - s_prod) / s_prod if s_prod > 0 else 0
                    if p_diff > 0.03:
                        incident = {"type": "Power Yield Deviation", "severity": "Critical", "details": f"Deviation: {round(p_diff*100,1)}% from site avg"}

                # Generic System rules (Comm/Trip)
                if not incident:
                    if pd.isna(p_val) and row_h >= 7 and row_h <= 19:
                        incident = {"type": "Communication Problem", "severity": "High", "details": "No data stream."}
                    elif p_val == 0 and s_prod > 2000 and row_h >= 7 and row_h <= 19:
                        incident = {"type": "Inverter Trip", "severity": "Critical", "details": "Zero production detected."}

                if incident:
                    # Append unique incidents
                    incident.update({"timestamp": f"{today_str} {row_time}", "inverter": inv})
                    all_incidents.append(incident)

        # De-duplicate: If the same inverter has the same error type for consecutive time slots,
        # we only keep the first one to avoid alert fatigue.
        unique_anomalies = []
        if all_incidents:
            all_incidents.sort(key=lambda x: (x['inverter'], x['type'], x['timestamp']))
            unique_anomalies.append(all_incidents[0])
            for k in range(1, len(all_incidents)):
                # If it's a different inverter or different problem, it's unique
                if (all_incidents[k]['inverter'] != all_incidents[k-1]['inverter'] or 
                    all_incidents[k]['type'] != all_incidents[k-1]['type']):
                    unique_anomalies.append(all_incidents[k])
                else:
                    # If same inverter and type, check time gap (Simplified: keep only if gap > 1 hour)
                    try:
                        t1 = datetime.strptime(all_incidents[k-1]['timestamp'], '%Y-%m-%d %H:%M')
                        t2 = datetime.strptime(all_incidents[k]['timestamp'], '%Y-%m-%d %H:%M')
                        if (t2 - t1).total_seconds() > 3600:
                            unique_anomalies.append(all_incidents[k])
                    except: pass

        dashboard_data = {
            "macro_health": {"total_inverters": len(inv_ids), "online": 0, "tripped": 0, "comms_lost": 0},
            "anomalies": unique_anomalies[-50:],
            "file_statuses": file_statuses
        }
        dashboard_data["macro_health"]["online"] = int((pot_df.iloc[-1][inv_ids] > 0).sum()) if not pot_df.empty else 0
        dashboard_data["macro_health"]["tripped"] = int((pot_df.iloc[-1][inv_ids] == 0).sum()) if not pot_df.empty else 0
        dashboard_data["macro_health"]["comms_lost"] = int(pot_df.iloc[-1][inv_ids].isna().sum()) if not pot_df.empty else 0

    # --- SAVE TIME-SERIES DAILY JSON ---
    today_str = datetime.now().strftime('%Y-%m-%d')
    daily_json_path = os.path.join(directory, f"dashboard_data_{today_str}.json")
    
    current_time_key = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    time_series_data = {}

    if os.path.exists(daily_json_path):
        try:
            with open(daily_json_path, 'r') as f:
                time_series_data = json.load(f)
        except (json.JSONDecodeError, ValueError) as e:
            logger.error(f"Corrupted JSON detected in {daily_json_path}. Creating new file. Error: {e}")
            time_series_data = {}

    time_series_data[current_time_key] = dashboard_data

    # Save back
    try:
        with open(daily_json_path, 'w') as f:
            json.dump(time_series_data, f, indent=4)
        logger.info(f"Analysis saved to daily JSON: {daily_json_path}")
    except Exception as e:
        logger.error(f"Failed to write JSON: {e}")

class VCOMHandler(FileSystemEventHandler):
    def __init__(self, directory):
        self.directory = directory
        self.last_run = 0

    def on_modified(self, event):
        if event.src_path.endswith('.xlsx'):
            if time.time() - self.last_run > 30:
                self.last_run = time.time()
                time.sleep(5)
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
