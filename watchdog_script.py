import time
import os
import sys
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from datetime import datetime

# Add data_analysis folder to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'data_analysis'))
from analysis_pipeline import analyze_inverter_data

class VCOMDataHandler(FileSystemEventHandler):
    def __init__(self, target_keywords, watch_dir):
        self.target_keywords = target_keywords
        self.watch_dir = watch_dir
        self.found_files = set()
        self.last_trigger_date = ""

    def on_created(self, event):
        self.process_event(event)

    def on_modified(self, event):
        self.process_event(event)

    def process_event(self, event):
        if event.is_directory:
            return
        
        filename = os.path.basename(event.src_path)
        if not filename.endswith('.xlsx'):
            return

        # Identify which target keyword this file matches
        matched_keyword = None
        for kw in self.target_keywords:
            if kw in filename:
                matched_keyword = kw
                break
        
        if matched_keyword:
            print(f"[Watchdog] Detected relevant file: {filename}")
            if self.wait_for_file_stability(event.src_path):
                self.found_files.add(matched_keyword)
                print(f"[Watchdog] File ready: {matched_keyword} (Current set: {self.found_files})")
                
                # If all 5 keywords are found, trigger analysis
                if len(self.found_files) >= 5:
                    today = datetime.now().strftime('%Y-%m-%d')
                    if self.last_trigger_date != today:
                        print(f"\n[Watchdog] SUCCESS: All 5 data sets acquired for {today}. Triggering Forensic Analysis...")

                        try:
                            analyze_inverter_data(self.watch_dir)
                            self.last_trigger_date = today
                            self.found_files.clear() # Reset for next batch/day
                        except Exception as e:
                            print(f"[Watchdog] Error during analysis: {e}")
                    else:
                        print("[Watchdog] Analysis already performed for today.")

    def wait_for_file_stability(self, filepath, timeout=30):
        """ Waits for file size to stop changing, ensuring write is complete. """
        start_time = time.time()
        last_size = -1
        
        while time.time() - start_time < timeout:
            try:
                current_size = os.path.getsize(filepath)
                if current_size == last_size and current_size > 0:
                    # Size stabilized
                    time.sleep(2) # Final buffer
                    return True
                last_size = current_size
            except OSError:
                # File might be locked or not yet available
                pass
            time.sleep(3)
        
        print(f"[Watchdog] Timeout waiting for file stability: {filepath}")
        return False

def start_monitoring():
    # Set relative path to extracted_data
    base_dir = os.path.dirname(os.path.abspath(__file__))
    watch_dir = os.path.join(base_dir, 'extracted_data')
    
    if not os.path.exists(watch_dir):
        os.makedirs(watch_dir)
        print(f"Created directory to watch: {watch_dir}")

    target_keywords = ['Potenza_AC', 'PR', 'Resistenza_Isolamento', 'Temperatura', 'Corrente_DC']

    
    event_handler = VCOMDataHandler(target_keywords, watch_dir)
    observer = Observer()
    observer.schedule(event_handler, watch_dir, recursive=False)
    
    print(f"--- Watchdog Online ---")
    print(f"Monitoring: {watch_dir}")
    print(f"Waiting for: {target_keywords}")
    
    observer.start()
    try:
        while True:
            time.sleep(5)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

if __name__ == "__main__":
    start_monitoring()