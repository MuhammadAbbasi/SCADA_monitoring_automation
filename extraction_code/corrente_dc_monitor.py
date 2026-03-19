import time
import pandas as pd


def extract_corrente_dc_data(page):
    """Extract Corrente DC data from the VCOM Evaluation dashboard."""
    print("\n--- Extracting Corrente DC Data ---")

    try:
        page.locator('text="Corrente DC"').first.click()

        # Click the "acceso" button for Valori in minuti if it's not active
        try:
            page.wait_for_selector('button[title="acceso"]:visible', timeout=30000)
            acceso_btn = page.locator('button[title="acceso"]:visible').first

            if 'active' not in acceso_btn.get_attribute('class'):
                print("Toggling 'Valori in minuti' to ACCESO...")
                acceso_btn.click()

                # Dismiss "Valori minimi non disponibili" modal if it appears
                try:
                    # Wait up to 12 seconds for a visible "Chiudi" button
                    page.wait_for_selector('button:has-text("Chiudi"):visible', timeout=12000)
                    print("Dismissing 'Valori minimi non disponibili' modal...")
                    page.locator('button:has-text("Chiudi"):visible').first.click()
                    time.sleep(2)  # wait for modal to disappear
                except Exception:
                    # Modal didn't appear or disappeared quickly
                    pass

                time.sleep(3)
            else:
                print("'Valori in minuti' is already ACCESO.")
        except Exception as e:
            print("Warning: Could not toggle 'Valori in minuti' (maybe not available for this metric).", e)

        try:
            if page.locator('button:has-text("Aggiorna grafico")').is_visible(timeout=5000):
                page.locator('button:has-text("Aggiorna grafico")').click()
        except Exception:
            pass

        # Click the "Dati" tab
        page.wait_for_selector('text="Dati"', timeout=20000)
        page.locator('text="Dati"').last.click()

        # Wait for the data table (if no rows exist, handle gracefully)
        rows_locator = page.locator('#infotab-data table tbody tr')
        try:
            rows_locator.first.wait_for(state="visible", timeout=20000)
        except Exception:
            print("No data rows found for Corrente DC (possibly due to unsupported minute resolution). Returning empty DataFrame.")
            return pd.DataFrame()

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
        print(f"Corrente DC Table Headers: {header_texts}")

        # Extract rows
        rows = rows_locator.all()
        print(f"Found {len(rows)} rows for Corrente DC.")

        dc_results = []
        for idx, row in enumerate(rows):
            # Extract text and ignore columns marked as SunGrow
            col_texts_raw = [text.strip() for text in row.locator('td').all_inner_texts()]
            col_texts = [v for i, v in enumerate(col_texts_raw) if i not in ignored_indices]

            # Trim data columns to match header length if there are extra empty columns
            if len(col_texts) > len(header_texts):
                col_texts = col_texts[:len(header_texts)]

            dc_results.append(col_texts)
            if idx < 5:
                print(f"Row {idx}: {col_texts}")

        if len(rows) > 5:
            print(f"... and {len(rows)-5} more rows.")

        # Create DataFrame
        df_dc = pd.DataFrame(dc_results, columns=header_texts)
        return df_dc

    except Exception as e:
        print(f"[ERROR] Unexpected error while extracting Corrente DC: {e}")
        return pd.DataFrame()
