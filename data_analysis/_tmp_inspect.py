import pandas as pd
import os

base = r'\\s01\get\2025.01 Mazara 01 A2A\03 - REPORT\Report\09 Testing\Automation\extracted_data'

files = {
    'PR': 'PR_2026-03-18.xlsx',
    'AC': 'Potenza_AC_2026-03-18.xlsx',
    'INS': 'Resistenza_Isolamento_2026-03-18.xlsx',
    'TEMP': 'Temperatura_2026-03-18.xlsx',
}

for key, fname in files.items():
    fpath = os.path.join(base, fname)
    df = pd.read_excel(fpath, nrows=10)
    print(f'\n=== {key} ({fname}) ===')
    print(f'Shape (first 10 rows): {df.shape}')
    print(f'Columns ({len(df.columns)}): {df.columns.tolist()}')
    print(df.head(5).to_string())
    print()

# Also show PR full (it's a summary file, likely small)
df_pr = pd.read_excel(os.path.join(base, 'PR_2026-03-18.xlsx'))
print('\n=== PR FULL ===')
print(f'Full shape: {df_pr.shape}')
print(df_pr.to_string())
