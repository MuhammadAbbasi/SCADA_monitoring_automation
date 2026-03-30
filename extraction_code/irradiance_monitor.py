import logging
import time
import pandas as pd

logger = logging.getLogger(__name__)

# Specific sensors list provided by user
IRR_SENSORS = [
    "JB-SM1_AL-1-DOWN", "JB-SM1_AL-1-UP", "JB-SM3_AL-3-DOWN", "JB-SM3_AL-3-UP",
    "JB-SM3_GHI-3", "JB1_GHI-1", "JB1_IT-1-1", "JB1_IT-1-2", "JB1_POA-1",
    "JB2_IT-2-1", "JB2_IT-2-2", "JB3_IT-3-1", "JB3_IT-3-2", "JB3_POA-3"
]

def extract_irradiance_data(page):
    """Extract Irraggiamento data for specific sensors."""
    logger.info("--- Extracting Irraggiamento Data (Sensors) ---")
    
    # 1. Navigate to Irraggiamento
    page.locator('text="Irraggiamento"').first.click()
    time.sleep(2)

    # 2. Toggle Valori in minuti if possible
    try:
        page.wait_for_selector('button[title="acceso"]:visible', timeout=10000)
        acceso_btn = page.locator('button[title="acceso"]:visible').first
        if 'active' not in acceso_btn.get_attribute('class'):
            logger.info("Toggling 'Valori in minuti' to ACCESO...")
            acceso_btn.click()
            try:
                page.wait_for_selector('button:has-text("Chiudi"):visible', timeout=5000)
                page.locator('button:has-text("Chiudi"):visible').first.click()
            except Exception: pass
            time.sleep(3)
    except Exception:
        logger.warning("Could not toggle 'Valori in minuti' for Irraggiamento.")

    # 3. Refresh chart
    try:
        aggiorna_btn = page.locator('button:has-text("Aggiorna grafico"), button:has-text("Update chart")')
        if aggiorna_btn.is_visible(timeout=3000):
            aggiorna_btn.click()
            time.sleep(2)
    except Exception: pass

    # 4. Click Dati tab
    page.wait_for_selector('text="Dati"', timeout=20000)
    page.locator('text="Dati"').last.click()

    # 5. Wait for table
    rows_locator = page.locator('#infotab-data table tbody tr')
    try:
        rows_locator.first.wait_for(state="visible", timeout=20000)
    except Exception:
        logger.warning("No data rows found for Irraggiamento.")
        return pd.DataFrame()

    # 6. Extract columns and rows
    headers = page.locator('#infotab-data table thead tr th').all()
    header_texts = [h.inner_text().strip() for h in headers]
    
    # Filter columns to only include Ora and our target sensors
    # (Sometimes headers include extra info, we try to match by sensor name)
    valid_cols = []
    col_indices = []
    for i, h in enumerate(header_texts):
        if h == "Ora" or any(s in h for s in IRR_SENSORS):
            valid_cols.append(h)
            col_indices.append(i)

    logger.info(f"Detected relevant Irradiance columns: {valid_cols}")

    rows = rows_locator.all()
    results = []
    for row in rows:
        cells = row.locator('td').all_inner_texts()
        filtered_cells = [cells[i].strip() for i in col_indices if i < len(cells)]
        
        # Apply Italian number conversion
        converted_cells = []
        for cell in filtered_cells:
            # Skip 'Ora'
            if len(converted_cells) == 0: 
                converted_cells.append(cell)
                continue
            try:
                # Replace comma with dot and remove thousands separator
                val = cell.replace('.', '').replace(',', '.')
                converted_cells.append(float(val))
            except ValueError:
                converted_cells.append(cell)
        
        results.append(converted_cells)

    df = pd.DataFrame(results, columns=valid_cols)
    return df
