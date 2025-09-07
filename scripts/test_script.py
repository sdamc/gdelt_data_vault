import os
import requests
import zipfile

timestamp = "20230901000000"
url = f"http://data.gdeltproject.org/gdeltv2/{timestamp}.export.CSV.zip"
output_dir = "./data/raw/gdelt_2_0/events"
os.makedirs(output_dir, exist_ok=True)

zip_path = os.path.join(output_dir, f"{timestamp}.zip")

print(f"Baixando: {url}")
response = requests.get(url, timeout=30)

if response.status_code == 200:
    print("Download bem-sucedido.")
    with open(zip_path, 'wb') as f:
        f.write(response.content)

    print("Extraindo...")
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(output_dir)

    os.remove(zip_path)
    print("✅ Arquivo salvo e extraído com sucesso.")
else:
    print(f"⚠️ Erro ao baixar: {response.status_code}")
