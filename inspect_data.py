import pandas as pd
import sys

file_path = "VCOM_Report_2026-03-17.xlsx"

try:
    xl = pd.ExcelFile(file_path)
    print("Sheets:", xl.sheet_names)
    
    for sheet in xl.sheet_names:
        print(f"\n--- Sheet: {sheet} ---")
        df = xl.parse(sheet, nrows=5)
        print(df)
except Exception as e:
    print("Error reading excel:", e)
