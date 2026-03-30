import subprocess
import os
import sys
import time
import threading

# Get absolute path to the project root
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

def log_reader(pipe, prefix):
    """Reads lines from a pipe and prints them with a prefix."""
    try:
        with pipe:
            for line in iter(pipe.readline, ''):
                print(f"[{prefix}] {line.strip()}")
    except EOFError:
        pass

def launch_subsystem(cmd_args, name):
    """Launches a python script in a subprocess and streams its output."""
    print(f"--- Starting Subsystem: {name} ---")
    process = subprocess.Popen(
        [sys.executable] + cmd_args,
        cwd=ROOT_DIR,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        encoding='utf-8',
        errors='replace'
    )
    
    # Start a thread to read logs for this process
    thread = threading.Thread(target=log_reader, args=(process.stdout, name))
    thread.daemon = True
    thread.start()
    return process

if __name__ == "__main__":
    processes = []
    
    print("\n" + "="*50)
    print("🚀 MAZARA MONITORING SYSTEM - LOCAL RUNNER")
    print("="*50 + "\n")

    try:
        # Start all 3 components
        processes.append(launch_subsystem(["dashboard/app.py"], "DASHBOARD"))
        processes.append(launch_subsystem(["processor_watchdog.py"], "WATCHDOG"))
        processes.append(launch_subsystem(["extraction_code/vcom_monitor.py"], "EXTRACTION"))

        print("\n✅ All systems live! Press Ctrl+C to terminate all processes.\n")
        print("📊 Dashboard available at: http://localhost:8080\n")

        # Keep main thread alive monitoring processes
        while True:
            for p in processes:
                if p.poll() is not None:
                    print(f"\n⚠️ WARNING: One of the subsystems crashed with exit code {p.returncode}")
                    break
            time.sleep(1)

    except KeyboardInterrupt:
        print("\n\n🛑 Termination signal received. Stopping all subsystems...")
        for p in processes:
            p.terminate()
        print("✅ Cleanup complete. Goodbye!\n")
