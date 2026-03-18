import pandas as pd
import os

# Path to extracted_data folder (one level up from data_analysis/)
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_ROOT_DIR = os.path.dirname(_SCRIPT_DIR)
_EXTRACTED_DATA_DIR = os.path.join(_ROOT_DIR, "extracted_data")

file_path = os.path.join(_EXTRACTED_DATA_DIR, "VCOM_Report_2026-03-17.xlsx")

try:
    xl = pd.ExcelFile(file_path)
    print("Sheets:", xl.sheet_names)

    for sheet in xl.sheet_names:
        print(f"\n--- Sheet: {sheet} ---")
        df = xl.parse(sheet, nrows=5)
        print(df)
except Exception as e:
    print("Error reading excel:", e)
