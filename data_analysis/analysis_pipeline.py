import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import glob
import os
from datetime import datetime

def clean_italian_localization(df):
    """ Converts Italian formatted numbers (comma decimals) to floats if they appear as strings. """
    # Remove Timestamp Fetch as it's not needed for merging
    if 'Timestamp Fetch' in df.columns:
        df = df.drop(columns=['Timestamp Fetch'])
    
    for col in df.columns:
        if col == 'Ora':
            continue
        if df[col].dtype == object:
            # Replace comma with dot and remove thousands separator (dot) if it's a string
            val = df[col].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
            df[col] = pd.to_numeric(val, errors='coerce')
    return df

def analyze_inverter_data(directory):
    print(f"\n--- Initializing Analysis for {directory} ---")
    
    # Identify files for the latest date (defaults to today)
    date_str = datetime.now().strftime("%Y-%m-%d")
    file_patterns = {
        'Potenza_AC': f"*Potenza_AC_{date_str}*.xlsx",
        'PR': f"*PR_{date_str}*.xlsx",
        'Resistenza_Isolamento': f"*Resistenza_Isolamento_{date_str}*.xlsx",
        'Temperatura': f"*Temperatura_{date_str}*.xlsx",
        'Corrente_DC': f"*Corrente_DC_{date_str}*.xlsx"
    }

    
    file_paths = {}
    for key, pattern in file_patterns.items():
        matches = glob.glob(os.path.join(directory, pattern))
        if not matches:
            print(f"[Warning] No file found for {key} with pattern {pattern}")
            continue
        file_paths[key] = matches[0]

    if len(file_paths) < 5:
        print("[Error] Missing required files for analysis.")
        return


    # Load and Clean
    print("Loading datasets...")
    pot_df = clean_italian_localization(pd.read_excel(file_paths['Potenza_AC']))
    res_df = clean_italian_localization(pd.read_excel(file_paths['Resistenza_Isolamento']))
    temp_df = clean_italian_localization(pd.read_excel(file_paths['Temperatura']))
    pr_df = clean_italian_localization(pd.read_excel(file_paths['PR']))
    dc_df = clean_italian_localization(pd.read_excel(file_paths['Corrente_DC']))


    # Standardize 'Ora' to string HH:MM if it's datetime
    for df in [pot_df, res_df, temp_df, dc_df]:

        if pd.api.types.is_datetime64_any_dtype(df['Ora']):
            df['Ora'] = df['Ora'].dt.strftime('%H:%M')

    # Merge on 'Ora'
    merged_ts = pot_df.merge(res_df, on='Ora', suffixes=('_POT', '_RES'))
    merged_ts = merged_ts.merge(temp_df, on='Ora', suffixes=('', '_TEMP'))
    merged_ts = merged_ts.merge(dc_df, on='Ora', suffixes=('', '_DC'))

    
    # Rename Temperatura columns if they don't have suffix
    temp_cols = [c for c in temp_df.columns if 'INV' in c and c != 'Ora']
    for c in temp_cols:
        if c in merged_ts.columns and c + '_TEMP' not in merged_ts.columns:
            merged_ts = merged_ts.rename(columns={c: c + '_TEMP'})

    # Rename Corrente DC columns if they don't have suffix
    dc_cols = [c for c in dc_df.columns if 'INV' in c and c != 'Ora']
    for c in dc_cols:
        if c in merged_ts.columns and c + '_DC' not in merged_ts.columns:
            merged_ts = merged_ts.rename(columns={c: c + '_DC'})


    inv_ids = [c.replace('_POT', '') for c in merged_ts.columns if '_POT' in c]
    
    # Site average for baseline
    pot_cols = [id + '_POT' for id in inv_ids]
    merged_ts['Site_Avg_POT'] = merged_ts[pot_cols].mean(axis=1)
    
    # Convert 'Ora' to numeric hours for calculations
    def to_hours(t):
        if isinstance(t, str):
            try:
                h, m = map(int, t.split(':')[:2])
                return h + m/60
            except: return np.nan
        return t

    merged_ts['Ora_Numeric'] = merged_ts['Ora'].apply(to_hours)
    
    daylight_mask = (merged_ts['Ora_Numeric'] >= 7.5) & (merged_ts['Ora_Numeric'] <= 17.5)
    df_day = merged_ts[daylight_mask].copy()

    # Results Containers
    trips = []
    underperformers = []
    late_starts = []
    thermal_derating = []
    comms_loss = []
    events = []

    print("Executing forensic analysis...")

    for inv in inv_ids:
        pot = df_day[inv + '_POT']
        res = df_day[inv + '_RES']
        temp = df_day[inv + '_TEMP']
        site_avg = df_day['Site_Avg_POT']
        
        # 1. Trip Analysis
        trip_points = df_day[(pot == 0) & (pot.shift(1) > 100) & (site_avg > 500)]
        if not trip_points.empty:
            for idx, row in trip_points.iterrows():
                t_time = row['Ora']
                # Analyze Pre-trip (30-60m)
                pre_trip = merged_ts[(merged_ts['Ora_Numeric'] < row['Ora_Numeric']) & 
                                     (merged_ts['Ora_Numeric'] >= row['Ora_Numeric'] - 1.0)]
                res_drop = pre_trip[inv + '_RES'].max() - pre_trip[inv + '_RES'].min()
                temp_spike = pre_trip[inv + '_TEMP'].max() - pre_trip[inv + '_TEMP'].min()
                
                diagnosis = "Suspected Trip"
                if res_drop > 200: diagnosis += " (Insulation Drop)"
                if temp_spike > 8: diagnosis += " (Overheating)"
                
                trips.append({'INV': inv, 'Time': t_time, 'Diagnosis': diagnosis})
                events.append(f"[{t_time}] {inv}: {diagnosis}. AC Power dropped to 0W. Pre-trip Res: {row[inv + '_RES']} kOhm, Temp: {row[inv + '_TEMP']} °C.")
                generate_plot(inv, merged_ts, row['Ora_Numeric'], directory)

        # 2. Underperformance Analysis
        if pot.mean() < 0.85 * site_avg.mean():
            # Cross-reference with PR file
            pr_val = "N/A"
            # Attempt to find the column in PR_df that matches INV ID
            matched_pr_cols = [c for c in pr_df.columns if inv in c]
            if matched_pr_cols:
                pr_val = pr_df[matched_pr_cols[0]].iloc[0]
            
            underperformers.append({'INV': inv, 'Production_Ratio': pot.mean()/site_avg.mean(), 'PR': pr_val})
            events.append(f"[Daily] {inv}: Underperforming ({(pot.mean()/site_avg.mean()*100):.1f}% of site avg). PR: {pr_val}.")

        # 3. Late Start
        start_points = pot[pot > 100]
        if not start_points.empty:
            start_time = df_day.loc[start_points.index[0], 'Ora_Numeric']
            site_start = df_day[site_avg > 100]['Ora_Numeric']
            if not site_start.empty:
                s_s = site_start.iloc[0]
                if start_time > s_s + 0.5:
                    late_starts.append({'INV': inv, 'Start': start_time, 'Site_Start': s_s})
                    events.append(f"[{merged_ts.loc[start_points.index[0], 'Ora']}] {inv}: Late wakeup. Site started at approx {s_s:.1f}h.")

        # 4. Thermal Derating
        # Temp > 65 and power dropping while site avg is steady/rising
        derate_pts = df_day[(temp > 65) & (pot.diff() < -50) & (site_avg.diff() >= -20)]
        if not derate_pts.empty:
            thermal_derating.append(inv)
            events.append(f"[Various] {inv}: Potential Thermal Derating detected at high temperatures (>65°C).")

        # 5. Comms Loss
        if df_day[inv + '_POT'].isnull().any():
            comms_loss.append(inv)
            events.append(f"[Various] {inv}: Data gaps (NaN) detected during daylight.")

    save_reports(trips, underperformers, late_starts, thermal_derating, comms_loss, events, directory)
    print("Analysis complete. Reports and charts saved to directory.")

def generate_plot(inv, df, trip_h, directory):
    fig, ax1 = plt.subplots(figsize=(12, 6))
    
    ax1.plot(df['Ora_Numeric'], df[inv + '_POT'], 'b-', linewidth=2, label='AC Power (W)')
    ax1.set_xlabel('Time (Hours)', fontweight='bold')
    ax1.set_ylabel('AC Power (W)', color='b', fontweight='bold')
    ax1.tick_params(axis='y', labelcolor='b')
    ax1.axvline(x=trip_h, color='orange', linestyle='--', linewidth=2, label='Shutdown Point')
    
    ax2 = ax1.twinx()
    ax2.plot(df['Ora_Numeric'], df[inv + '_RES'], 'r--', alpha=0.7, label='Insulation (kOhm)')
    ax2.plot(df['Ora_Numeric'], df[inv + '_TEMP'], 'g-.', alpha=0.7, label='Temperature (°C)')
    ax2.set_ylabel('Res (kOhm) / Temp (°C)', fontweight='bold')
    
    # Legend
    lines, labels = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines + lines2, labels + labels2, loc='upper left')
    
    plt.title(f'Forensic Analysis Event: {inv}', fontweight='bold', fontsize=14)
    plt.grid(True, alpha=0.3)
    fig.tight_layout()
    plt.savefig(os.path.join(directory, f"{inv.replace(' ', '_')}_correlation_chart.png"))
    plt.close()

def save_reports(trips, under, late, thermal, comms, events, directory):
    # Executive Summary
    summary = "# Executive Summary - Inverter Anomaly Detection\n\n"
    summary += f"**Processed on:** {datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n"
    
    summary += "### Critical Issues (Trips)\n"
    if trips:
        for t in trips:
            summary += f"- **{t['INV']}**: Tripped at {t['Time']}. Diagnosis: {t['Diagnosis']}.\n"
    else:
        summary += "- No critical trips detected.\n"
        
    summary += "\n### Underperforming Units\n"
    if under:
        for u in under:
            summary += f"- **{u['INV']}**: Production at {(u['Production_Ratio']*100):.1f}% of site avg. PR: {u['PR']}.\n"
    else:
        summary += "- All units producing within normal range.\n"

    summary += "\n### Other Anomalies\n"
    if late: summary += f"- **Late Starts**: {', '.join(set([x['INV'] for x in late]))}\n"
    if thermal: summary += f"- **Thermal Derating**: {', '.join(set(thermal))}\n"
    if comms: summary += f"- **Comms Loss**: {', '.join(set(comms))}\n"

    with open(os.path.join(directory, 'Executive_Summary.md'), 'w') as f:
        f.write(summary)
        
    # Detailed Event Log
    with open(os.path.join(directory, 'Detailed_Event_Log.md'), 'w') as f:
        f.write("# Detailed Event Log\n\n")
        f.write("\n".join([f"- {e}" for e in events]))

if __name__ == "__main__":
    # Locate data relative to script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(os.path.dirname(script_dir), 'extracted_data')
    analyze_inverter_data(data_dir)