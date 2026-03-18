import time
import pandas as pd


def extract_temperature_data(page):
    """Extract Inverter Temperature data from the VCOM Evaluation dashboard."""
    print("\n--- Extracting Temperature Data ---")
    # Click 'Temperatura' or similar equivalent under 'Inverter'
    page.wait_for_selector('text="Temperatura"', timeout=30000)
    page.locator('text="Temperatura"').first.click()

    # Click the "acceso" button for Valori in minuti if it's not active
    try:
        page.wait_for_selector('button[title="acceso"]:visible', timeout=10000)
        acceso_btn = page.locator('button[title="acceso"]:visible').first

        if 'active' not in acceso_btn.get_attribute('class'):
            print("Toggling 'Valori in minuti' to ACCESO...")
            acceso_btn.click()
            
            # Dismiss "Valori minimi non disponibili" modal if it appears
            try:
                # Wait up to 8 seconds for a visible "Chiudi" button
                page.wait_for_selector('button:has-text("Chiudi"):visible', timeout=8000)
                print("Dismissing 'Valori minimi non disponibili' modal...")
                page.locator('button:has-text("Chiudi"):visible').first.click()
                time.sleep(2) # wait for modal to disappear
            except:
                pass
                
            time.sleep(3)
        else:
            print("'Valori in minuti' is already ACCESO.")
    except Exception as e:
        print("Could not toggle 'Valori in minuti' (maybe not available for this metric).", e)

    try:
        if page.locator('button:has-text("Aggiorna grafico")').is_visible(timeout=5000):
            page.locator('button:has-text("Aggiorna grafico")').click()
    except:
        pass

    # Click the "Dati" tab
    page.wait_for_selector('text="Dati"', timeout=20000)
    page.locator('text="Dati"').last.click()

    # Wait for the data table
    page.wait_for_selector('#infotab-data table tbody tr', timeout=20000)

    # Extract headers
    headers = page.locator('#infotab-data table thead tr th').all()
    header_texts_raw = [h.inner_text().strip() for h in headers]
    
    # Filter out 'SunGrow SG350HX' columns
    header_texts = []
    ignored_indices = []
    for i, h in enumerate(header_texts_raw):
        if "SunGrow SG350HX" in h:
            ignored_indices.append(i)
        else:
            header_texts.append(h)
    print(f"Temperature Table Headers: {header_texts}")

    # Extract rows
    rows = page.locator('#infotab-data table tbody tr').all()
    print(f"Found {len(rows)} rows for Temperature.")

    results = []

    for idx, row in enumerate(rows):
        # Extract text and ignore columns marked as SunGrow
        col_texts_raw = [text.strip() for text in row.locator('td').all_inner_texts()]
        col_texts = [v for i, v in enumerate(col_texts_raw) if i not in ignored_indices]
        
        # Trim data columns to match header length if there are extra empty columns
        if len(col_texts) > len(header_texts):
            col_texts = col_texts[:len(header_texts)]
            
        results.append(col_texts)
        if idx < 5:
            print(f"Row {idx}: {col_texts}")

    print(f"... and {len(rows)-5} more rows.")
    
    # Create DataFrame
    df = pd.DataFrame(results, columns=header_texts)
    return df
