import pandas as pd
import glob
import os

files = glob.glob('extracted_data/*.xlsx')
if not files:
    print("No files found")
else:
    for f in files[:2]:
        print(f"Checking {f}...")
        df = pd.read_excel(f, nrows=5)
        print(df.head())
        for col in df.columns:
            if df[col].dtype == object:
                s = df[col].astype(str)
                if s.str.contains(',').any():
                    print(f"Commas found in {col}")
                if s.str.contains('\.').any():
                    print(f"Dots found in {col}")
