import configparser
import psycopg2
import mysql.connector
import logging

def connect_to_database():
    # Lê o arquivo de configuração
    config = configparser.ConfigParser()
    config.read('config.properties')

    database_type = config.get('database', 'database')

    try:
        # Conectar ao banco de dados PostgreSQL
        if database_type == 'postgresql':
            conn = psycopg2.connect(
                host=config.get('postgresql', 'postgresql.host'),
                port=config.get('postgresql', 'postgresql.port'),
                database=config.get('postgresql', 'postgresql.database'),
                user=config.get('postgresql', 'postgresql.user'),
                password=config.get('postgresql', 'postgresql.password')
            )
            cursor = conn.cursor()

        # Conectar ao banco de dados MySQL
        elif database_type == 'mysql':
            conn = mysql.connector.connect(
                host=config.get('mysql', 'mysql.host'),
                port=config.get('mysql', 'mysql.port'),
                database=config.get('mysql', 'mysql.database'),
                user=config.get('mysql', 'mysql.user'),
                password=config.get('mysql', 'mysql.password')
            )
            cursor = conn.cursor()

        else:
            logging.error("Tipo de banco de dados inválido no arquivo de configuração.")
            return None, None, False

        # Configurar o logger para exibir mensagens no console
        logging.basicConfig(level=logging.INFO)

        # Registrar a confirmação da conexão no log e exibir no console
        logging.info("Conexão com o banco de dados " + database_type + " estabelecida com sucesso!")
        print("Conexão com o banco de dados", database_type, "estabelecida com sucesso!")

        # Retornar conexão e cursor
        return conn, cursor, True

    except (Exception, psycopg2.Error, mysql.connector.Error) as error:
        logging.error("Erro ao conectar ao banco de dados: " + str(error))
        print("Erro ao conectar ao banco de dados:", error)
        return None, None, False
