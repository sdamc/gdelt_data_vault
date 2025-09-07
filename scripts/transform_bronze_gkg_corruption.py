import os
import pandas as pd

def filter_gkg_for_corruption(input_dir="./data/raw/gdelt_2_0/gkg", output_dir="./data/bronze/gkg/corrupcao"):
    os.makedirs(output_dir, exist_ok=True)
    arquivos = sorted([f for f in os.listdir(input_dir) if f.endswith(".gkg.csv")])

    for arquivo in arquivos:
        input_path = os.path.join(input_dir, arquivo)
        output_path = os.path.join(output_dir, arquivo)

        if os.path.exists(output_path):
            print(f"🟡 Já existe: {arquivo}")
            continue

        try:
            df = pd.read_csv(input_path, sep="\t", header=None, dtype=str, encoding='utf-8', low_memory=False)

            # Filtra onde a coluna 8 (V2Themes) contém "Corruption"
            df_corrupcao = df[df[8].str.contains("Corruption", na=False)]

            if not df_corrupcao.empty:
                df_corrupcao.to_csv(output_path, sep="\t", index=False, header=False)
                print(f"✅ Salvo: {arquivo} — {len(df_corrupcao)} linhas")
            else:
                print(f"⚠️ Sem corrupção: {arquivo}")

        except Exception as e:
            print(f"❌ Erro ao processar {arquivo}: {e}")
