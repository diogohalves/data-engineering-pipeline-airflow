import csv

INPUT_FILE = "/opt/airflow/scripts/clientes_raw.csv"
OUTPUT_FILE = "/opt/airflow/scripts/clientes_tratados.csv"

def run():
    print("Iniciando TRANSFORM...")

    clientes_validos = []

    with open(INPUT_FILE, mode="r", encoding="utf-8") as entrada:
        leitor = csv.DictReader(entrada)

        for linha in leitor:
            idade = int(linha["idade"])

            if idade <= 0:
                continue

            clientes_validos.append(linha)
    
    with open(OUTPUT_FILE, mode="w", newline="", encoding="utf-8") as saida:
        campos = clientes_validos[0].keys()
        escritor = csv.DictWriter(saida, fieldnames=campos)
        escritor.writeheader()
        escritor.writerows(clientes_validos)
    
    print(f"TRANSFORM concluído. Registros válidos: {len(clientes_validos)}")

if __name__ == "__main__":
    run()