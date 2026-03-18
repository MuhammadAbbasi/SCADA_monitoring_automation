import time
import os
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import sys

# Ensure data_analysis module can be imported
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'data_analysis'))
from analysis_pipeline import analyze_inverter_data

class VCOMDataHandler(FileSystemEventHandler):
    def __init__(self, target_files, watch_dir):
        self.target_files = target_files
        self.watch_dir = watch_dir
        self.updated_files = set()

    def on_modified(self, event):
        if event.is_directory:
            return
        
        filename = os.path.basename(event.src_path)
        if any(target in filename for target in self.target_files):
            # Safeguard: Wait for the file to finish writing
            if self.is_file_ready(event.src_path):
                self.updated_files.add(filename)
                print(f"[Watchdog] {filename} fully updated.")
                
            # Check if all 4 required files are ready
            if len(self.updated_files) >= 4:
                print("\n[Watchdog] All 4 files acquired. Triggering Analysis Phase...")
                # Pass the directory to the analysis function
                analyze_inverter_data(self.watch_dir)
                self.updated_files.clear() # Reset for the next day

    def is_file_ready(self, filepath):
        """ Checks if the file size is stable to ensure download/writing is complete. """
        try:
            initial_size = os.path.getsize(filepath)
            time.sleep(3)
            final_size = os.path.getsize(filepath)
            return initial_size == final_size and initial_size > 0
        except Exception:
            return False

def start_watchdog(path_to_watch):
    target_files = ['Potenza_AC', 'PR', 'Resistenza_Isolamento', 'Temperatura']
    event_handler = VCOMDataHandler(target_files, path_to_watch)
    observer = Observer()
    observer.schedule(event_handler, path=path_to_watch, recursive=False)
    observer.start()
    print(f"Watchdog started. Monitoring {path_to_watch} for VCOM exports...")
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

if __name__ == "__main__":
    DOWNLOAD_DIR = "/extracted_data/"
    start_watchdog(DOWNLOAD_DIR)