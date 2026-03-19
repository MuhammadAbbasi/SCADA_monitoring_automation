import time
import pandas as pd


def extract_irradiance_data(page):
    """Extract Irraggiamento data from the VCOM Evaluation dashboard."""
    print("\n--- Extracting Irraggiamento Data ---")
    page.locator('text="Irraggiamento"').first.click()

    # Click the "acceso" button for Valori in minuti if it's not active (when available)
    try:
        page.wait_for_selector('button[title="acceso"]:visible', timeout=10000)
        acceso_btn = page.locator('button[title="acceso"]:visible').first

        if 'active' not in acceso_btn.get_attribute('class'):
            print("Toggling 'Valori in minuti' to ACCESO...")
            acceso_btn.click()

            # Dismiss "Valori minimi non disponibili" modal if it appears
            try:
                page.wait_for_selector('button:has-text("Chiudi"):visible', timeout=12000)
                print("Dismissing 'Valori minimi non disponibili' modal...")
                page.locator('button:has-text("Chiudi"):visible').first.click()
                time.sleep(2)
            except Exception:
                pass

            time.sleep(3)
        else:
            print("'Valori in minuti' is already ACCESO.")
    except Exception as e:
        print("Warning: could not toggle 'Valori in minuti' for Irraggiamento (maybe not available).", e)

    # Refresh chart if button exists
    try:
        if page.locator('button:has-text("Aggiorna grafico")').is_visible(timeout=5000):
            page.locator('button:has-text("Aggiorna grafico")').click()
    except Exception:
        pass

    # Click the "Dati" tab
    page.wait_for_selector('text="Dati"', timeout=20000)
    page.locator('text="Dati"').last.click()

    # Wait for the data table
    rows_locator = page.locator('#infotab-data table tbody tr')
    try:
        rows_locator.first.wait_for(state="visible", timeout=20000)
    except Exception:
        print("No data rows found for Irraggiamento (possibly unsupported resolution). Returning empty DataFrame.")
        return pd.DataFrame()

    # Extract headers
    headers = page.locator('#infotab-data table thead tr th').all()
    header_texts = [h.inner_text().strip() for h in headers]

    # Extract rows
    rows = rows_locator.all()
    print(f"Found {len(rows)} rows for Irraggiamento.")

    results = []
    for idx, row in enumerate(rows):
        col_texts = [text.strip() for text in row.locator('td').all_inner_texts()]
        # Trim to header length
        if len(col_texts) > len(header_texts):
            col_texts = col_texts[:len(header_texts)]
        results.append(col_texts)
        if idx < 5:
            print(f"Row {idx}: {col_texts}")

    df = pd.DataFrame(results, columns=header_texts)
    return df
