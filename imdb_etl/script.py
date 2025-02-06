"""
Script de Processamento de Dados (Versão 2.0 - Atualizado)
Este é o script mais recente e deve ser utilizado pelos usuários.
Para mais detalhes, consulte o README.md.
"""

import os
import sqlite3
import logging

import pandas as pd
import requests

# Configuração do Logging
log_format = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.DEBUG, format=log_format)

DB = "imdb_data.db" # Banco de Dados
BASE_URL = "https://datasets.imdbws.com/" # url_base para os datasets do IMDB
BASE_DIR = "Data" # diretório destino
FILES = {
        "name.basics.tsv.gz",
        "title.akas.tsv.gz",
        "title.basics.tsv.gz",
        "title.crew.tsv.gz",
        "title.episode.tsv.gz",
        "title.principals.tsv.gz",
        "title.ratings.tsv.gz"
    }  # Nomes dos arquivos que desejamos baixar

### EXTRAÇÃO DOS DADOS
def extract(
    base_url: str = BASE_URL,
    base_dir: str = BASE_DIR,
    files:    list = FILES
) -> None:

    """
    Downloads files from a specified base URL to a local directory.
    Args:
        base_url (str): The base URL from which to download the files.
        base_dir (str): The local directory where the files will be saved.
        files (list): A list of filenames to be downloaded.
    Returns:
        None
    The function ensures that the target directory exists, then iterates over the list of files.
    For each file, it checks if the file already exists in the target directory to avoid redundant downloads.
    If the file does not exist, it downloads the file from the specified URL and saves it to the target directory.
    Logs are generated to indicate the progress and status of each download.
    """

    os.makedirs(base_dir, exist_ok=True) # Certifique-se de que o diretório destino existe

    #Loop para baixar os arquivos
    for filename in files:
        url = base_url + filename
        namepath = os.path.join(base_dir, filename)
        
        # verifica se o arquivo já existe para evitar o download repetido
        if not os.path.exists(namepath):
            logging.info(f"Baixando {filename}...")
            response = requests.get(url)
            
            # verifica se a solicitação foi bem sucedida
            if response.status_code == 200:
                with open(namepath, 'wb') as f:
                    f.write(response.content)
                logging.info(f"{filename} baixado com sucesso")
            else:
                logging.error(f"Falha ao baixar {filename}. Código de status: {response.status_code}")
        else:
            logging.info(f"{filename} já existe.")
    logging.info(f"Download concluído.")

### TRANSFORMAÇÃO DOS DADOS
def transform(
    base_dir: str = BASE_DIR
) -> None:
    
    """
    Transforms and processes TSV files in the specified base directory.
    This function reads all gzip-compressed TSV files in the given base directory,
    replaces specific null value representations, and saves the processed files
    in a subdirectory named 'processed'.
    Args:
        base_dir (str): The base directory containing the TSV files to be processed.
                        Defaults to BASE_DIR.
    Returns:
        None
    Raises:
        OSError: If there is an issue creating the 'processed' directory or reading files.
    Example:
        transform("/path/to/base_dir")
    Notes:
        - The function assumes that the TSV files are compressed using gzip.
        - The function replaces occurrences of the string "\\N" with None.
        - Processed files are saved without compression in the 'processed' directory.
    """

    processed_dir = os.path.join(base_dir, "processed") # diretório dos arquivos extraídos e modificados
    os.makedirs(processed_dir, exist_ok=True) # Certifique-se de que o diretório processed existe
    
    files = os.listdir(base_dir) # Lista todos os arquivos do diretório Data

    # Loop para abrir, tratar e salvar cada arquivo
    for filename in files:
        namepath = os.path.join(base_dir, filename)
        
        if os.path.isfile(namepath) and filename.endswith("gz"):
            logging.debug(f"Lendo e tratando arquivo {filename}...")
            
            # Lê o arquivo TSV usando pandas
            df = pd.read_csv(namepath, sep='\t', compression='gzip', low_memory=False)
            
            # Substitui os caracteres "ln" um valor nulo
            df.replace({"\\N": None}, inplace=True)
            
            # Salva o DataFrame no diretório "processed" sem compressão
            output_file_path = os.path.join(processed_dir, filename[:-3])  # Caminho completo para o arquivo processado
            df.to_csv(output_file_path, sep='\t', index=False)
            
            logging.debug(f"Tratamento concluído para {filename}. Arquivo tratado e salvo em {output_file_path}")
    logging.info(f"Todos os arquivos foram tratados e salvos no diretório Processed")

### CRIAÇÃO DAS TABELAS ANALÍTICAS
def create_views(conn):
    analytics_title = """
        CREATE TABLE IF NOT EXISTS table_analytics AS
        WITH
            participants AS (
                SELECT
                        tp.tconst,
                        COUNT (DISTINCT tp.nconst) as QtParticipants
                    FROM title_principals tp
                    GROUP BY 1
            )
        SELECT
            tb.tconst,
            tb.titleType,
            tb.originalTitle,
            tb.startYear,
            tb.endYear,
            tb.genres,
            tr.averageRating,
            tr.numVotes,
            tp.QtParticipants
        FROM title_basics tb
        LEFT JOIN title_ratings tr     ON tb.tconst = tr.tconst
        LEFT JOIN participants tp      ON tp.tconst = tb.tconst
    """

    analytics_participants = """
        CREATE TABLE IF NOT EXISTS table_participants AS
            SELECT 
                tp.nconst,
                tb.tconst,
                tp.ordering,  
                tp.category,                         
                tb.genres
            FROM title_principals tp
            LEFT JOIN title_basics tb   ON tp.tconst = tb.tconst
    """
    # Lista de consultas
    queries = [
        "DROP TABLE IF EXISTS analytics_title",
        "DROP TABLE IF EXISTS analytics_participants",
        analytics_title, 
        analytics_participants
    ]
    for q in queries:

        #bd = "imdb_data.db"      
        #conn = sqlite3.connect(db) # Conecta-se com o banco de dados

        q = q
        
        conn.execute(q) #  Execução
        #conn.close() # Fecha a conexão
    
    logging.info("Tabelas analíticas criadas com sucesso!")

### CARGA DOS DADOS
def load(
    conn
) -> None:
    
    """
    Loads TSV files from the 'Data/processed' directory into a SQLite database.
    Args:
        conn: SQLite database connection object.
    Returns:
        None
    This function reads all TSV files in the 'Data/processed' directory, processes each file,
    and saves the data into a corresponding table in the SQLite database. The table name is
    derived from the filename by removing the extension and replacing special characters
    ('.' and '-') with underscores ('_'). If a table with the same name already exists, it
    will be replaced. The function logs the progress of each file processed and indicates
    when all files have been saved to the database.
    """

    processed_dir = os.path.join("Data", "processed") # Diretórios
    files = os.listdir(processed_dir) # Lista todos os arquivos no diretório processed

    # Loop para ler cada arquivo e salvar em uma tabela SQLite
    for filename in files:
        output_file_path = os.path.join(processed_dir, filename)

        if os.path.isfile(output_file_path) and filename.endswith(".tsv"):
            # Lê o arquivo tsv usando o pandas
            df = pd.read_csv(output_file_path, sep='\t', low_memory=False)

            # O nome da tabela é o nome do arquivo sem a extensão do nome
            table_name = os.path.splitext(filename)[0]

            # Substitui os caracteres especiais no nome da tabela
            table_name = table_name.replace(".", "_").replace("-", "_")

            # Salva o Dataframe na tabela SQLite
            df.to_sql(table_name, conn, index=False, if_exists='replace')

            logging.info(f"Aquivo {filename} salvo como tabela {table_name} no banco de dados")

    # Fecha a conexão com o banco de dados
    
    logging.info("Todos os arquivos foram salvos no banco de dados.")

def main():
    """
    Main function to execute the ETL process.

    This function performs the following steps:
    1. Extracts data by calling the `extract` function.
    2. Transforms data by calling the `transform` function.
    3. Connects to the SQLite database specified by the `DB` variable.
    4. Loads data into the database by calling the `load` function.
    5. Creates views in the database by calling the `create_views` function.
    6. Closes the database connection.
    7. Logs a success message upon completion.

    Raises:
        sqlite3.Error: If there is an error connecting to or interacting with the SQLite database.
    """
    extract()
    transform()
    conn = sqlite3.connect(DB) # Conecta ao banco de dados SQLite
    load(conn)
    create_views(conn)
    conn.close()  
    logging.info("Finalizado com sucesso!")

if __name__ == "__main__":
    main()

# Agende a execução do script
""" schedule.every().day.at("09:00").do(execute_script)

while True:
    schedule.run_pending()
    time.sleep(1) """