### Importações
import os
import requests
import pandas as pd
import sqlite3
import logging
import schedule
import time
import shutil

"""
Script de Processamento de Dados (Versão 1.0 - Legado)
Esta é uma versão antiga e não deve ser mais utilizada.
Mantido apenas para referência histórica.
"""

# Configuração do Logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def execute_script():
    ### EXTRAÇÃO DOS DADOS
    
    base_url="https://datasets.imdbws.com/" # url_base para os datasets do IMDB

    base_dir = "Data" # diretório destino
    os.makedirs(base_dir, exist_ok=True) # Certifique-se de que o diretório destino existe

    # Nomes dos arquivos que desejamos baixar
    files = {
        "name.basics.tsv.gz",
        "title.akas.tsv.gz",
        "title.basics.tsv.gz",
        "title.crew.tsv.gz",
        "title.episode.tsv.gz",
        "title.principals.tsv.gz",
        "title.ratings.tsv.gz"
    }

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

    ### CARGA DOS DADOS

    db = "imdb_data.db" # Banco de Dados
    conn = sqlite3.connect(db) # Conecta ao banco de dados SQLite

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
    conn.close()
    logging.info("Todos os arquivos foram salvos no banco de dados.")

    ### CRIAÇÃO DAS TAABELAS ANALÍTICAS

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
                tb.tconst,
                tp.ordering,  
                tp.category,                         
                tb.genres
            FROM title_principals tp
            LEFT JOIN title_basics tb   ON tp.tconst = tb.tconst
    """
    # Lista de consultas
    queries = [analytics_title, analytics_participants]
    for q in queries:

        bd = "imdb_data.db"      
        conn = sqlite3.connect(db) # Conecta-se com o banco de dados

        q = q
        conn.execute(q) #  Execução
        conn.close() # Fecha a conexão
    
    logging.info("Tabelas criadas com sucesso!")

execute_script()
        
# Agende a execução do script
""" schedule.every().day.at("09:00").do(execute_script)

while True:
    schedule.run_pending()
    time.sleep(1) """