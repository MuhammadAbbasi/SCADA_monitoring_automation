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
    """Returns the latest plant health data from the daily time-series JSON file."""
    today_str = datetime.now().strftime('%Y-%m-%d')
    daily_data_path = os.path.join(_ROOT_DIR, "extracted_data", f"dashboard_data_{today_str}.json")

    if not os.path.exists(daily_data_path):
        # Return empty structure if no data for today yet
        return {}
    
    try:
        with open(daily_data_path, 'r') as f:
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
