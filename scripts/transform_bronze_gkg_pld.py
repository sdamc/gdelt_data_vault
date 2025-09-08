import os
import pandas as pd
import re

def filter_gkg_for_pld(
    input_dir="./data/raw", 
    output_dir="./data/bronze/",
    delete_after_transform=True
    ):

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
            df_pld = df[df[8].str.contains("ECON_MONEYLAUNDERING", na=False)]

            if not df_pld.empty:
                df_pld.to_csv(output_path, sep="\t", index=False, header=False)
                print(f"✅ Salvo: {arquivo} — {len(df_pld)} linhas")
            
            if delete_after_transform:
                os.remove(input_path)
                print(f"🗑️ Deletado: {arquivo}")
            else:
                print(f"⚠️ Sem pld: {arquivo}")

        except Exception as e:
            print(f"❌ Erro ao processar {arquivo}: {e}")
