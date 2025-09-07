import os
import requests
import zipfile
from io import BytesIO
from datetime import datetime, timedelta
from tqdm import tqdm

def download_gdelt_events(start_date, end_date, output_dir="./data/raw"):
    os.makedirs(output_dir, exist_ok=True)

    current_date = start_date
    while current_date <= end_date:
        yyyymmdd = current_date.strftime("%Y%m%d")
        url = f"http://data.gdeltproject.org/events/{yyyymmdd}.export.CSV.zip"
        local_csv = os.path.join(output_dir, f"{yyyymmdd}.csv")

        if os.path.exists(local_csv):
            current_date += timedelta(days=1)
            continue

        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                with zipfile.ZipFile(BytesIO(response.content)) as z:
                    csv_file = z.namelist()[0]
                    with z.open(csv_file) as f_in, open(local_csv, 'wb') as f_out:
                        f_out.write(f_in.read())
                print(f"✔ Downloaded {yyyymmdd}")
            else:
                print(f"✖ No file for {yyyymmdd}")
        except Exception as e:
            print(f"⚠ Error on {yyyymmdd}: {e}")

        current_date += timedelta(days=1)

# Exemplo de uso
if __name__ == "__main__":
    start = datetime(2015, 1, 1)
    end = datetime(2015, 1, 10)
    download_gdelt_events(start, end)
