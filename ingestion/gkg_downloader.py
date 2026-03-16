import os
import requests
import zipfile
import io
import pandas as pd
import argparse
from datetime import datetime, timedelta
import state_table as meta

DATASET = "gkg"
BRONZE_PATH = os.getenv("BRONZE_PATH", "/workspace/data/bronze")
MAX_FILES_PER_RUN = 17280  # ~6 months of data (180 days * 96 files/day)
THEMES = ["ECON_MONEYLAUNDERING", "WB_2076_MONEY_LAUNDERING"]

def get_next_ts(current_ts: int) -> int:
    if current_ts == 0:
        return 20260101000000  # Start from 2026-01-01 
    dt = datetime.strptime(str(current_ts), "%Y%m%d%H%M%S")
    next_dt = dt + timedelta(minutes=15)
    return int(next_dt.strftime("%Y%m%d%H%M%S"))

def run_gdelt_ingestion(run_id: str, max_files: int = 17280, output_path: str = None, start_from_ts: int = None):
    conn = meta.get_connection()
    output_path = output_path or BRONZE_PATH
    os.makedirs(output_path, exist_ok=True)
    
    # Initialize metadata tables if they don't exist
    try:
        meta.create_state_tables(conn)
        print("✅ Metadata tables initialized")
    except Exception as e:
        print(f"⚠️  Warning: Could not initialize metadata tables: {e}")

    # Check watermark
    meta.init_run(conn, run_id, DATASET, max_files_per_run=max_files)
    last_watermark = meta.get_last_watermark(conn, DATASET)
    
    # Priority: explicit start_from_ts > ensure starting from 2026 > watermark
    if start_from_ts is not None:
        current_ts = start_from_ts
        print(f"🎯 Using explicit start timestamp: {current_ts}")
    elif last_watermark == 0 or last_watermark < 20260101000000:
        current_ts = 20260101000000
        print(f"🎯 Starting from 2026-01-01 (watermark was {last_watermark})")
    else:
        current_ts = get_next_ts(last_watermark)
        print(f"🔄 Continuing from watermark: {current_ts}")
    
    total_rows_saved = 0
    files_processed = 0
    files_downloaded = 0
    files_failed = 0

    print(f"🚀 Starting Run {run_id} from timestamp {current_ts}")
    print(f"🎯 Target: {max_files:,} files (~{max_files/96:.0f} days)")

    try:
        while files_processed < max_files:
            filename = f"{current_ts}.gkg.csv.zip"
            url = f"http://data.gdeltproject.org/gdeltv2/{current_ts}.gkg.csv.zip"
            meta.insert_file_record(conn, DATASET, current_ts, url, filename, run_id, 'queued')
            meta.update_file_status(conn, DATASET, current_ts, 'downloading')
            
            files_processed += 1

            try:
                response = requests.get(url, timeout=20)
                if response.status_code == 200:                    
                    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                        with z.open(z.namelist()[0]) as f:
                            df = pd.read_csv(f, sep="\t", header=None, dtype=str, encoding='utf-8', encoding_errors='ignore')
                            mask = df[8].str.contains('|'.join(THEMES), na=False)
                            df_filtered = df[mask]
                            row_count = len(df_filtered)
                            if row_count > 0:
                                out_path = os.path.join(output_path, f"{current_ts}.bronze.csv")
                                df_filtered.to_csv(out_path, sep="\t", index=False, header=False)
                                total_rows_saved += row_count
                                files_downloaded += 1
                                meta.update_file_status(conn, DATASET, current_ts, 'downloaded', bytes_downloaded=len(response.content))
                            else:
                                meta.update_file_status(conn, DATASET, current_ts, 'skipped', error_message="No AML themes found")
                            meta.update_watermark(conn, DATASET, current_ts, run_id)
                            if files_processed % 500 == 0:
                                print(f"📊 Progress: {files_processed:,}/{max_files:,} files ({files_processed/max_files*100:.1f}%) | {files_downloaded:,} with AML data, {total_rows_saved:,} rows | {current_ts}")
                elif response.status_code == 404:
                    current_dt = datetime.strptime(str(current_ts), "%Y%m%d%H%M%S")
                    if current_dt > datetime.now():
                        print(f"⌛ Reached current time - no more GDELT files available at {current_ts}")
                        meta.update_file_status(conn, DATASET, current_ts, 'skipped', error_message="404 - Future date")
                        break
                    else:
                        print(f"⚠️  File not found (404): {current_ts} - skipping...")
                        meta.update_file_status(conn, DATASET, current_ts, 'skipped', error_message="404 Not Found")
                        meta.update_watermark(conn, DATASET, current_ts, run_id)  
                else:
                    raise Exception(f"HTTP {response.status_code}")

            except Exception as e:
                print(f"❌ Error on {current_ts}: {e}")
                files_failed += 1
                meta.update_file_status(conn, DATASET, current_ts, 'failed', error_message=str(e))

            current_ts = get_next_ts(current_ts)
        
        # Download summary
        print(f"\n✅ Download Complete:")
        print(f"   Files processed: {files_processed:,}")
        print(f"   Files with AML data: {files_downloaded:,} ({files_downloaded/files_processed*100:.1f}%)")
        print(f"   Total rows saved: {total_rows_saved:,}")
        print(f"   Files failed: {files_failed:,}")
        print(f"   Time coverage: ~{files_processed/96:.0f} days")
        print(f"   Final timestamp: {current_ts}")
        
        # Determine final status based on failures
        final_status = 'partial' if files_failed > 0 else 'success'
        
        meta.update_run_status(
            conn, run_id, final_status, 
            files_downloaded=files_downloaded, 
            files_failed=files_failed,
            files_detected=files_processed,
            files_selected=files_downloaded
        )

    except Exception as run_error:
        meta.update_run_status(conn, run_id, 'failed', error_message=str(run_error))
    finally:
        conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Download GDELT GKG files filtered for AML themes')
    parser.add_argument('--output', type=str, default=BRONZE_PATH, help='Output directory for bronze CSV files')
    parser.add_argument('--max-files', type=int, default=MAX_FILES_PER_RUN, help='Maximum files to download per run (default: 17,280 = ~6 months)')
    parser.add_argument('--reset-watermark', action='store_true', help='Reset watermark to 0 (script will auto-start from 2026-01-01)')
    parser.add_argument('--start-from', type=str, help='Start from specific timestamp (format: YYYYMMDDHHMMSS), overrides watermark')
    args = parser.parse_args()
    
    if args.reset_watermark:
        conn = meta.get_connection()
        meta.create_state_tables(conn)  
        
        start_ts = 0  # Will auto-start from 20260101000000
        print(f"🔄 Resetting watermark to 0 (script will auto-start from 2026-01-01)")
        
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO meta.ingestion_watermark (dataset, last_successful_file_ts, updated_at)
                VALUES (%s, %s, NOW())
                ON CONFLICT (dataset) DO UPDATE 
                SET last_successful_file_ts = EXCLUDED.last_successful_file_ts,
                    updated_at = NOW()
            """, (DATASET, start_ts))
        conn.commit()
        conn.close()
        print(f"✅ Watermark reset complete\n")
    
    print(f"🎯 Configuration: max_files={args.max_files:,} (~{args.max_files/96:.0f} days), output={args.output}")
    
    start_ts = None
    if args.start_from:
        start_ts = int(args.start_from)
    
    run_gdelt_ingestion(
        run_id=f"manual_{datetime.now().strftime('%Y%m%d%H%M%S')}",
        max_files=args.max_files,
        output_path=args.output,
        start_from_ts=start_ts
    )