import time
import pandas as pd


def extract_pr_data(page):
    """Extract Performance Ratio data from the VCOM Evaluation dashboard."""
    print("\n--- Extracting PR Inverter Data ---")
    page.locator('text="PR inverter"').first.click()

    # If the "Componenti" picker is open, click "Aggiorna grafico"
    try:
        if page.locator('button:has-text("Aggiorna grafico")').is_visible(timeout=5000):
            page.locator('button:has-text("Aggiorna grafico")').click()
    except:
        pass

    # Wait for the "Dati" tab to appear below the chart and click it
    page.wait_for_selector('text="Dati"', timeout=30000)
    page.locator('text="Dati"').last.click()

    # Wait for the data table and extract
    page.wait_for_selector('table#measuredValues tbody tr, #infotab-data table tbody tr', timeout=20000)

    rows = page.locator('table#measuredValues tbody tr').all()
    if not rows:
        rows = page.locator('#infotab-data table tbody tr').all()

    print(f"Found {len(rows)} rows for PR Inverter.")

    pr_results = []
    for row in rows:
        columns = row.locator('td').all()
        if len(columns) >= 2:
            inv_name = columns[0].inner_text().strip()
            
            if "SunGrow SG350HX" in inv_name:
                continue
                
            pr_val_str = columns[1].inner_text().strip()
            
            # Format PR value to float for Excel
            try:
                pr_val = float(pr_val_str.replace(',', '.'))
            except ValueError:
                pr_val = pr_val_str
                
            pr_results.append({'Inverter': inv_name, 'PR': pr_val})
            print(f"  {inv_name}: PR {pr_val_str}%")
            
    df_pr = pd.DataFrame(pr_results)
    return df_pr
