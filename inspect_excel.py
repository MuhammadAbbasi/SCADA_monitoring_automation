"""Quick inspection script to check number formatting in extracted Excel files."""
import pandas as pd
import os
import glob

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_EXTRACTED_DATA_DIR = os.path.join(_SCRIPT_DIR, "extracted_data")

files_to_check = [
    "Potenza_AC_*.xlsx",
    "PR_*.xlsx",
    "Resistenza_Isolamento_*.xlsx",
    "Temperatura_*.xlsx",
    "Corrente_DC_*.xlsx",
    "Irraggiamento_*.xlsx",
]

for pattern in files_to_check:
    matches = glob.glob(os.path.join(_EXTRACTED_DATA_DIR, pattern))
    if not matches:
        print(f"\n[SKIP] No files for pattern: {pattern}")
        continue
    
    # Use the newest file
    latest = max(matches, key=os.path.getctime)
    print(f"\n{'='*60}")
    print(f"FILE: {os.path.basename(latest)}")
    print(f"{'='*60}")
    
    try:
        df = pd.read_excel(latest, nrows=3)
        print(f"Columns ({len(df.columns)}): {list(df.columns)}")
        print(f"\nData types:")
        print(df.dtypes.to_string())
        print(f"\nFirst 3 rows (raw):")
        print(df.to_string())
        
        # Check for comma-formatted strings
        has_commas = False
        for col in df.columns:
            if df[col].dtype == object:
                sample_vals = df[col].dropna().head(3).tolist()
                comma_vals = [v for v in sample_vals if isinstance(v, str) and ',' in v]
                if comma_vals:
                    has_commas = True
                    print(f"\n  [!] Column '{col}' has COMMA values: {comma_vals}")
        
        if not has_commas:
            print(f"\n  [OK] No comma-formatted strings detected in first 3 rows.")
    except Exception as e:
        print(f"  [ERROR] {e}")

print("\n\nDone.")
