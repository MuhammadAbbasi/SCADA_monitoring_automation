import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import glob
import os

def clean_italian_localization(df):
    """ Converts Italian formatted numbers (comma decimals, dot thousands) to standard floats. """
    df = df.dropna(axis=1, how='all')
    if 'Timestamp Fetch' in df.columns:
        df = df.drop(columns=['Timestamp Fetch']) # Not needed for timeseries merge
        
    for col in df.columns:
        if ('INV' in col or 'PR' in col) and df[col].dtype == object:
            # Remove thousand separators, then replace decimal comma with dot
            df[col] = df[col].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False).astype(float)
    return df

def analyze_inverter_data(directory):
    print("Initializing Data Cleaning and Merging...")
    
    # Locate latest files (Excel format)
    pot_file = glob.glob(os.path.join(directory, '*Potenza_AC*.xlsx'))[0]
    pr_file = glob.glob(os.path.join(directory, '*PR*.xlsx'))[0]
    res_file = glob.glob(os.path.join(directory, '*Resistenza_Isolamento*.xlsx'))[0]
    temp_file = glob.glob(os.path.join(directory, '*Temperatura*.xlsx'))[0]

    # Load and clean Data
    pot_df = clean_italian_localization(pd.read_excel(pot_file)).groupby('Ora').mean().reset_index()
    res_df = clean_italian_localization(pd.read_excel(res_file)).groupby('Ora').mean().reset_index()
    temp_df = clean_italian_localization(pd.read_excel(temp_file)).groupby('Ora').mean().reset_index()
    pr_df = clean_italian_localization(pd.read_excel(pr_file))

    inv_cols = [c for c in pot_df.columns if 'INV' in c]
    inv_names = [c.split('(')[1].split(')')[0] for c in inv_cols]

    # Mask for daylight generation
    daylight_mask = (pot_df['Ora'] >= 6.00) & (pot_df['Ora'] <= 18.00)
    pot_day = pot_df.loc[daylight_mask].copy()
    avg_power = pot_day[inv_cols].mean(axis=1)

    tripped_inverters = []
    underperforming_inverters = []

    print("\n--- Running Forensic Anomaly Detection ---")
    
    # 1. Trip Analysis (Morning Ramp -> sudden 0W)
    for col, inv in zip(inv_cols, inv_names):
        series = pot_day[col]
        # Condition: Previous timestamp generated >100W, but current dropped to 0W while fleet is healthy (>2000W)
        trip_mask = (series == 0) & (series.shift(1) > 100) & (avg_power > 2000)
        
        if trip_mask.any():
            trip_time = pot_day.loc[trip_mask.idxmax(), 'Ora']
            tripped_inverters.append(inv)
            generate_multiaxis_plot(inv, pot_df, res_df, temp_df, trip_time, directory)
            
    # 2. Underperformance Analysis (15% below average + PR check)
    for col, inv in zip(inv_cols, inv_names):
        if inv in tripped_inverters: continue
        if pot_day[col].mean() < 0.85 * avg_power.mean():
            underperforming_inverters.append(inv)
            
    print(f"Critical Trips Detected: {tripped_inverters}")
    print(f"Underperforming Units: {underperforming_inverters}")
    print("Analysis Complete. Multiaxis charts saved to directory.")

def generate_multiaxis_plot(inv, pot_df, res_df, temp_df, trip_time, directory):
    """ Generates and saves the correlation charts requested in Phase 3. """
    pot_col = [c for c in pot_df.columns if inv in c][0]
    res_col = [c for c in res_df.columns if inv in c][0]
    temp_col = [c for c in temp_df.columns if inv in c][0]
    
    fig, ax1 = plt.subplots(figsize=(12,6))
    
    # Primary Axis: AC Power
    ax1.plot(pot_df['Ora'], pot_df[pot_col], 'b-', linewidth=2, label='AC Power (W)')
    ax1.set_xlabel('Time of Day (Ora)', fontweight='bold')
    ax1.set_ylabel('AC Power (W)', color='b', fontweight='bold')
    ax1.tick_params('y', colors='b')
    ax1.axvline(x=trip_time, color="#FFA600", linestyle='--', linewidth=2, label='Shutdown Trigger')
    
    # Secondary Axis: Insulation Resistance & Temperature
    ax2 = ax1.twinx()
    ax2.plot(res_df['Ora'], res_df[res_col], 'r--', linewidth=2, label='Insulation Res. (kOhm)')
    ax2.plot(temp_df['Ora'], temp_df[temp_col], 'g-.', linewidth=2, label='Internal Temp (°C)')
    ax2.set_ylabel('Resistance (kOhm) / Temp (°C)', color='k', fontweight='bold')
    
    lines, labels = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines + lines2, labels + labels2, loc='upper left')
    
    plt.title(f'Forensic Analysis: {inv} Failure Event', fontweight='bold', fontsize=14)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(os.path.join(directory, f'{inv}_correlation_chart.png'))
    plt.close()

if __name__ == "__main__":
    # Can be run manually independently of watchdog
    script_dir = os.path.dirname(os.path.abspath(__file__))
    extracted_data_dir = os.path.join(os.path.dirname(script_dir), 'extracted_data')
    analyze_inverter_data(extracted_data_dir)