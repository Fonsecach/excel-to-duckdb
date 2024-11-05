import pandas as pd
import duckdb
import os


def excel_to_duckdb(excel_file, sheet_name, table_name, db_file='database.duckdb'):
    """
    Lê uma planilha Excel e salva os dados em uma tabela DuckDB.
    Cria o banco de dados automaticamente se ele não existir.

    Parâmetros:
    excel_file (str): Caminho do arquivo Excel
    sheet_name (str): Nome da aba da planilha
    table_name (str): Nome da tabela que será criada no DuckDB
    db_file (str): Nome do arquivo do banco de dados DuckDB
    """
    try:
        # Verifica se o arquivo Excel existe
        if not os.path.exists(excel_file):
            raise FileNotFoundError(f"O arquivo Excel '{excel_file}' não foi encontrado!")

        # Verifica se o banco já existe
        db_exists = os.path.exists(db_file)
        if not db_exists:
            print(f"Banco de dados '{db_file}' não existe. Será criado automaticamente.")
        else:
            print(f"Conectando ao banco de dados existente: '{db_file}'")

        # Lê a planilha Excel
        print(f"Lendo a planilha '{sheet_name}' do arquivo '{excel_file}'...")
        df = pd.read_excel(excel_file, sheet_name=sheet_name)

        # Verificação de dados vazios
        if df.empty:
            raise ValueError("A planilha está vazia!")

        # Conecta ao DuckDB (cria se não existir)
        print(f"{'Criando e conectando' if not db_exists else 'Conectando'} ao banco de dados...")
        con = duckdb.connect(db_file)

        # Cria a tabela e insere os dados
        print(f"Criando tabela '{table_name}' e inserindo dados...")
        con.execute(f"DROP TABLE IF EXISTS {table_name}")
        con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")

        # Confirma o número de registros e mostra informações da tabela
        result = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        print(f"\nOperação concluída com sucesso!")
        print(f"- Registros inseridos: {result[0]}")

        # Mostra informações sobre as colunas
        schema = con.execute(f"DESCRIBE {table_name}").fetchall()
        print("- Estrutura da tabela:")
        for col in schema:
            print(f"  - {col[0]}: {col[1]}")

        # Fecha a conexão
        con.close()

    except Exception as e:
        print(f"Erro durante a execução: {str(e)}")
        raise


if __name__ == "__main__":

    excel_file = "consolidado.xlsx"
    sheet_name = "Sheet1"
    table_name = "minha_tabela"
    db_file = "meu_banco.duckdb"

    excel_to_duckdb(excel_file, sheet_name, table_name, db_file)