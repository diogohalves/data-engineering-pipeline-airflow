import csv

INPUT_FILE = "/opt/airflow/scripts/clientes_incremental.csv"
OUTPUT_FILE = "/opt/airflow/scripts/clientes_raw.csv"

def run():
    print("Iniciando EXTRACT...")

    with open(INPUT_FILE, mode="r", encoding="utf-8") as entrada:
        leitor = csv.DictReader(entrada)
        dados = list(leitor)

    with open(OUTPUT_FILE, mode="w", newline="", encoding="utf-8") as saida:
        campos = dados[0].keys()
        escritor = csv.DictWriter(saida, fieldnames=campos)
        escritor.writeheader()
        escritor.writerows(dados)
    
    print(f"Extract concluído. Registros extraídos: {len(dados)}")
    return len(dados)

if __name__ == "__main__":
    run()

