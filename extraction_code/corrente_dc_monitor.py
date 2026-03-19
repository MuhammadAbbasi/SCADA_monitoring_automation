import time
import pandas as pd

def extract_corrente_dc_data(page):
    """Extract Corrente DC data from the VCOM Evaluation dashboard."""
    print("\n--- Extracting Corrente DC Data ---")

    try:
        # 1. Click the metric tab
        page.locator('text="Corrente DC"').first.click()

        # 2. IMMEDIATE MODAL CHECK: Switching tabs inherits the previous 1-min setting,
        # which instantly triggers the modal for SunGrow inverters. Handle it immediately.
        try:
            page.wait_for_selector('button:has-text("Chiudi"):visible', timeout=3000)
            print("[Log] Resolution warning detected after tab switch. Clicking 'Chiudi'...")
            page.locator('button:has-text("Chiudi"):visible').first.click()
            time.sleep(2)  # Wait for modal to fade and UI to adjust to 5-min intervals
        except Exception:
            # Modal didn't appear, which is fine.
            pass

        # 3. Attempt to turn on "Valori in minuti" if the system allows it
        try:
            # Wait briefly to let the UI settle
            time.sleep(1)
            if page.locator('button[title="acceso"]:visible').count() > 0:
                acceso_btn = page.locator('button[title="acceso"]:visible').first
                
                # Check if it's already active
                class_attr = acceso_btn.get_attribute('class')
                if class_attr and 'active' not in class_attr:
                    print("Toggling 'Valori in minuti' to ACCESO...")
                    acceso_btn.click()

                    # 4. SECOND MODAL CHECK: Forcing 1-min might trigger the error again.
                    try:
                        page.wait_for_selector('button:has-text("Chiudi"):visible', timeout=3000)
                        print("[Log] 1-minute resolution rejected by VCOM. Clicking 'Chiudi' and using lowest available resolution...")
                        page.locator('button:has-text("Chiudi"):visible').first.click()
                        time.sleep(2)
                    except Exception:
                        pass
                else:
                    print("'Valori in minuti' is already ACCESO.")
        except Exception as e:
            print("Warning: Could not toggle 'Valori in minuti' (maybe not available for this metric).", e)

        # 5. Refresh graph if prompted
        try:
            if page.locator('button:has-text("Aggiorna grafico")').is_visible(timeout=3000):
                page.locator('button:has-text("Aggiorna grafico")').click()
                time.sleep(2)
        except Exception:
            pass

        # 6. Go to the "Dati" (Data) tab
        print("Navigating to 'Dati' tab...")
        page.wait_for_selector('text="Dati"', timeout=10000)
        page.locator('text="Dati"').last.click()

        # 7. Wait for the data table to populate
        rows_locator = page.locator('#infotab-data table tbody tr')
        try:
            rows_locator.first.wait_for(state="visible", timeout=15000)
        except Exception:
            print("[Warning] No data rows found for Corrente DC. Returning empty DataFrame.")
            return pd.DataFrame()

        # 8. Extract headers
        headers = page.locator('#infotab-data table thead tr th').all()
        header_texts_raw = [h.inner_text().strip() for h in headers]

        # Filter out 'SunGrow SG350HX' warning headers if they appear as columns
        header_texts = []
        ignored_indices = []
        for i, h in enumerate(header_texts_raw):
            if "SunGrow SG350HX" in h:
                ignored_indices.append(i)
            else:
                header_texts.append(h)
                
        print(f"Corrente DC Table Headers: {header_texts}")

        # 9. Extract rows
        rows = rows_locator.all()
        print(f"Found {len(rows)} rows for Corrente DC.")

        dc_results = []
        for idx, row in enumerate(rows):
            # Extract text and ignore columns marked as SunGrow warning
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

        # 10. Create and return DataFrame
        df_dc = pd.DataFrame(dc_results, columns=header_texts)
        return df_dc

    except Exception as e:
        print(f"[ERROR] Unexpected error while extracting Corrente DC: {e}")
        return pd.DataFrame()