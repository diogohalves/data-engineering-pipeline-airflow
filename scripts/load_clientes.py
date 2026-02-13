import csv
from db import get_connection

INPUT_FILE = "/opt/airflow/scripts/clientes_tratados.csv"

def run():
    print("Iniciando LOAD...")

    conexao = get_connection()
    cursor = conexao.cursor()

    try:
        dados = []

        with open(INPUT_FILE, mode="r", encoding="utf-8") as entrada:
            leitor = csv.DictReader(entrada)

            for linha in leitor:
                dados.append((
                    int(linha["id"]),
                    linha["nome"],
                    int(linha["idade"]),
                    linha["pais"]
                ))

        sql = """
        INSERT INTO clientes (id, nome, idade, pais)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (id)
        DO UPDATE SET
            nome = EXCLUDED.nome,
            idade = EXCLUDED.idade,
            pais = EXCLUDED.pais;
        """

        cursor.executemany(sql, dados)
        conexao.commit()

        print(f"LOAD concluído. Registros carregados: {len(dados)}")
    
    except Exception as erro:
        conexao.rollback()
        print("Erro no LOAD:", erro)
        raise
    
    finally:
        cursor.close()
        conexao.close()

if __name__ == "__main__":
    run()