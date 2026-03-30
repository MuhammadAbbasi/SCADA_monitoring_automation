import logging
import time
import pandas as pd

logger = logging.getLogger(__name__)

def extract_corrente_dc_data(page):
    """Extract Corrente DC data from the VCOM Evaluation dashboard."""
    logger.info("--- Extracting Corrente DC Data ---")
    
    # 1. Navigate to Corrente DC
    page.locator('text="Corrente DC"').first.click()
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
        logger.warning("Could not toggle 'Valori in minuti' for Corrente DC.")

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
        logger.warning("No data rows found for Corrente DC.")
        return pd.DataFrame()

    # 6. Extract columns and rows
    headers = page.locator('#infotab-data table thead tr th').all()
    header_texts_raw = [h.inner_text().strip() for h in headers]
    
    header_texts = []
    ignored_indices = []
    for i, h in enumerate(header_texts_raw):
        if "SunGrow" in h:
            ignored_indices.append(i)
        else:
            header_texts.append(h)

    logger.info(f"Corrente DC Headers: {header_texts}")

    rows = rows_locator.all()
    results = []
    for row in rows:
        cells = row.locator('td').all_inner_texts()
        cells = [cells[i].strip() for i in range(len(cells)) if i not in ignored_indices]
        
        converted_cells = []
        for cell in cells:
            if len(converted_cells) == 0: 
                converted_cells.append(cell)
                continue
            try:
                val = cell.replace('.', '').replace(',', '.')
                converted_cells.append(float(val))
            except ValueError:
                converted_cells.append(cell)
        
        results.append(converted_cells)

    df = pd.DataFrame(results, columns=header_texts)
    return df