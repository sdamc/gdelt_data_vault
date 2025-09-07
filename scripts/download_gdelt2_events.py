import os
import requests
import zipfile
from datetime import datetime, timedelta

def generate_timestamps(start_date, end_date, interval_minutes=15):
    """
    Gera timestamps no formato GDELT 2.0 entre duas datas.
    """
    current = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)
    while current < end:
        yield current.strftime("%Y%m%d%H%M%S")
        current += timedelta(minutes=interval_minutes)

def download_gdelt2_events(start_date, end_date, output_dir="./data/raw/gdelt_2_0/events"):
    """
    Faz o download e extração dos arquivos GDELT 2.0 Events.
    """
    os.makedirs(output_dir, exist_ok=True)

    for timestamp in generate_timestamps(start_date, end_date):
        url = f"http://data.gdeltproject.org/gdeltv2/{timestamp}.export.CSV.zip"
        zip_path = os.path.join(output_dir, f"{timestamp}.zip")
        csv_path = os.path.join(output_dir, f"{timestamp}.export.CSV")

        if os.path.exists(csv_path):
            print(f"✅ Já existe: {csv_path}")
            continue

        try:
            response = requests.get(url, timeout=30)
            if response.status_code == 200:
                print(f"⬇️ Baixando: {timestamp}")
                with open(zip_path, 'wb') as f:
                    f.write(response.content)
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(output_dir)
                os.remove(zip_path)
                print(f"✅ Sucesso: {timestamp}")
            else:
                print(f"⚠️ Não encontrado: {timestamp}")
        except Exception as e:
            print(f"❌ Erro: {timestamp} — {e}")
