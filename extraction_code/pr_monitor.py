import logging
import time
import pandas as pd

logger = logging.getLogger(__name__)

def extract_pr_data(page):
    """Extract PR Inverter data from the VCOM Evaluation dashboard."""
    logger.info("--- Extracting PR Inverter Data ---")
    
    # 1. Navigate to PR Inverter
    page.locator('text="PR inverter"').first.click()
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
        logger.warning("Could not toggle 'Valori in minuti' for PR Inverter.")

    # 3. Refresh chart
    try:
        aggiorna_btn = page.locator('button:has-text("Aggiorna grafico"), button:has-text("Update chart")')
        if aggiorna_btn.is_visible(timeout=3000):
            aggiorna_btn.click()
            time.sleep(2)
    except Exception: pass

    # 4. Wait for the "Dati" tab to appear
    page.wait_for_selector('text="Dati"', timeout=30000)
    page.locator('text="Dati"').last.click()

    # 5. Extract rows
    page.wait_for_selector('table#measuredValues tbody tr, #infotab-data table tbody tr', timeout=20000)
    rows = page.locator('table#measuredValues tbody tr').all()
    if not rows:
        rows = page.locator('#infotab-data table tbody tr').all()

    logger.info(f"Found {len(rows)} rows for PR Inverter.")

    pr_results = []
    for row in rows:
        col_texts = row.locator('td').all_inner_texts()
        if len(col_texts) >= 2:
            inv_name = col_texts[0].strip()
            if "SunGrow" in inv_name: continue

            pr_val_str = col_texts[1].strip()
            try:
                pr_val = float(pr_val_str.replace('.', '').replace(',', '.'))
            except ValueError:
                pr_val = pr_val_str

            pr_results.append({'Inverter': inv_name, 'PR': pr_val})

    df_pr = pd.DataFrame(pr_results)
    return df_pr
