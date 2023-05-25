import psycopg2
import json

def connect_to_database():
    # Lê o arquivo de configuração
    with open('config.json') as config_file:
        config = json.load(config_file)

    try:
        # Conectar ao banco de dados PostgreSQL
        conn = psycopg2.connect(
            host=config['host'],
            port=config['port'],
            database=config['database'],
            user=config['user'],
            password=config['password']
        )

        cursor = conn.cursor()

        # Retornar conexão e cursor
        return conn, cursor, True

    except (Exception, psycopg2.Error) as error:
        print("Erro ao conectar ao banco de dados:", error)
        return None, None, False
    
# Utilização da função
conn, cursor, connected = connect_to_database()
