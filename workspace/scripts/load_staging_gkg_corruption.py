import os
import psycopg2

# Configurações
BRONZE_DIR = "./data/raw/bronze"
TABLE_NAME = "staging_gkg_corruption"

# Arquivos de entrada
def list_gkg_files():
    try:
        return sorted([f for f in os.listdir(BRONZE_DIR) if f.endswith(".gkg.csv")])
    except FileNotFoundError:
        print(f"❌ Diretório não encontrado: {BRONZE_DIR}")
        return []

# Conexão com o PostgreSQL
def get_connection():
    return psycopg2.connect(
        host="dv_postgres",
        port=5432,
        dbname="gdelt",
        user="gdelt_user",
        password="gdelt_pass"
    )

# Criação da tabela (executar apenas uma vez ou usar IF NOT EXISTS)
def create_table_if_not_exists(conn):
    with conn.cursor() as cur:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                id SERIAL PRIMARY KEY,
                filename TEXT,
                date_id BIGINT,
                gkg_record TEXT
            );
        """)
        conn.commit()
        print(f"✅ Tabela {TABLE_NAME} verificada/criada.")

# Inserção de dados
def insert_gkg_file(conn, file_path, filename):
    date_id = int(filename.split(".")[0])  # ex: 20160101000000.gkg.csv → 20160101000000
    with open(file_path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    with conn.cursor() as cur:
        for line in lines:
            cur.execute(
                f"INSERT INTO {TABLE_NAME} (filename, date_id, gkg_record) VALUES (%s, %s, %s)",
                (filename, date_id, line.strip())
            )
    conn.commit()
    print(f"📥 Inserido: {filename} — {len(lines)} linhas")

# Pipeline principal
def load_bronze_to_staging():
    files = list_gkg_files()
    if not files:
        print("⚠️ Nenhum arquivo para processar.")
        return

    conn = get_connection()
    create_table_if_not_exists(conn)

    for filename in files:
        file_path = os.path.join(BRONZE_DIR, filename)
        insert_gkg_file(conn, file_path, filename)

    conn.close()
    print("🏁 Processo concluído.")

# Execução direta via terminal
if __name__ == "__main__":
    load_bronze_to_staging()
