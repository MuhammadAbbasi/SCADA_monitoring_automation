from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import os
import json
import uvicorn
from datetime import datetime

# Paths
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_ROOT_DIR = os.path.dirname(_SCRIPT_DIR)
_DATA_PATH = os.path.join(_ROOT_DIR, "extracted_data", "dashboard_data.json")

app = FastAPI(title="Mazara Monitoring Dashboard")

# Serve static files
static_path = os.path.join(_SCRIPT_DIR, "static")
if not os.path.exists(static_path):
    os.makedirs(static_path)

@app.get("/api/status")
async def get_status():
    """Returns the latest plant health data from the JSON file."""
    if not os.path.exists(_DATA_PATH):
        # Empty placeholder if no data exists yet
        return {
            "last_updated": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "macro_health": {"total_inverters": 0, "online": 0, "tripped": 0, "comms_lost": 0},
            "anomalies": [],
            "historical_alarms": []
        }
    
    try:
        with open(_DATA_PATH, 'r') as f:
            return json.load(f)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/")
async def read_index():
    return FileResponse(os.path.join(static_path, "index.html"))

# Mount static directory for JS/CSS
app.mount("/static", StaticFiles(directory=static_path), name="static")

if __name__ == "__main__":
    # Bind to 0.0.0.0:8080 as requested to allow local network access
    uvicorn.run(app, host="0.0.0.0", port=8080)
