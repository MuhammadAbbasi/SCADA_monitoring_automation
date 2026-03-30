import sys
import os
import logging

# Add current directory to path to import processor_watchdog
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from processor_watchdog import analyze_site

# Setup minimal logging to stdout
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

if __name__ == "__main__":
    target_dir = "./extracted_data"
    print(f"--- MANUAL ANALYSIS TRIGGER ---")
    print(f"Targeting: {target_dir}")
    try:
        analyze_site(target_dir)
        print("\n✅ Analysis complete! Dashboard should now show the latest data.")
    except Exception as e:
        print(f"\n❌ Error during manual analysis: {e}")
