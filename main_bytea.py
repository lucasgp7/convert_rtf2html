import os
import datetime
import aspose.words as aw
import logging
from bs4 import BeautifulSoup
from connect import *

def convert_rtf_to_html(rtf_file, html_file):
    doc = aw.Document(rtf_file)
    doc.save(html_file)

# Utilização da função de conexão do banco
conn, cursor, connected = connect_to_database()
cursor = conn.cursor()

# Criar um objeto ConfigParser, ler o config.properties e pega o valor do database
config = configparser.ConfigParser()
config.read('config.properties')
database_type = config.get('database','database')
# Valor do schema pois só existe schema no postgres no mysql é database apenas
if database_type == 'postgresql':
    schema = config.get('schema', 'schema')
elif database_type == 'mysql':
    schema = config.get('mysql', 'mysql.database')


# Definir a tabela de entrada e a tabela de saída
input_table = schema + ".laudo_legado"
output_table = schema + ".laudo_import_html"

# Configurar o logger
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s')

# Verificar o tipo de banco de dados para criar a tabela
if database_type == 'postgresql':
    create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {output_table}(
            id SERIAL PRIMARY KEY,
            id_laudo VARCHAR,
            texto TEXT,
            error_message TEXT,
            dt_import_laudo TIMESTAMP,
            convertido BOOLEAN DEFAULT FALSE 
        )
"""
elif database_type == 'mysql':
    create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {output_table}(
                id INT AUTO_INCREMENT PRIMARY KEY,
                id_laudo VARCHAR(255),
                texto TEXT,
                error_message TEXT,
                dt_import_laudo DATETIME,
                convertido BOOLEAN DEFAULT FALSE 
            )
        """
else:
    raise ValueError("Tipo de banco de dados inválido")
# Criar a tabela de saída de acordo com o banco escolhido
cursor.execute(create_table_query)
conn.commit()


# Adicionar um índice na coluna "id_laudo" da tabela de saída de acordo com banco escolhido
if database_type == 'postgresql':
    add_index_query = f"CREATE INDEX IF NOT EXISTS idx_id_laudo ON {output_table} (id_laudo)"
    cursor.execute(add_index_query)
    conn.commit()
elif database_type == 'mysql':
    # Verificar se o índice já existe
    existing_index_query = f"SHOW INDEX FROM {output_table} WHERE Key_name = 'idx_id_laudo'"
    cursor.execute(existing_index_query)
    existing_index = cursor.fetchone()

    if existing_index is None:
        # O índice não existe, então pode ser criado
        add_index_query = f"ALTER TABLE {output_table} ADD INDEX idx_id_laudo (id_laudo)"
        cursor.execute(add_index_query)
        conn.commit()
    else:
        # O índice já existe
        logging.info("O índice 'idx_id_laudo' já existe na tabela.")


# Ler os dados da tabela de entrada
select_query = f"SELECT idlaudo, laudo FROM {input_table} WHERE idlaudo IS NOT NULL AND idlaudo != ''"
cursor.execute(select_query)
rows = cursor.fetchall()

# Obter o último id_laudo lido a partir do log (se existir)
ultimo_id_laudo_lido = None

# Iterar pelas linhas da tabela de entrada
for idlaudo, laudo in rows:
    if idlaudo == ultimo_id_laudo_lido:
        # Encontrado o último id_laudo lido, continuar para o próximo
        continue

# Obter a data e hora atual
current_datetime = datetime.datetime.now()

errors = []  # Lista para armazenar os erros encontrados

# Converter cada arquivo RTF para HTML e salvar na tabela de saída
for row in rows:
    file_id, rtf_text = row
    rtf_file = f"input_{file_id}.rtf"

    # Verificar se o ID já foi convertido
    check_query = "SELECT id_laudo FROM " + output_table + " WHERE id_laudo = %(file_id)s"
    cursor.execute(check_query, {"file_id": file_id})
    result = cursor.fetchone()

    if result:
        error_message = f"Arquivo ID: {file_id} Laudo já convertido"
        update_query = f"UPDATE {output_table} SET error_message = %(error_message)s WHERE id_laudo = %(file_id)s"
        cursor.execute(update_query, {"error_message": error_message, "file_id": file_id})
        conn.commit()
        errors.append((file_id, error_message, current_datetime))
        continue

    # Salvar o conteúdo do RTF em um arquivo temporário
    if database_type == 'postgresql':
        with open(rtf_file, "wb") as file:
            file.write(rtf_text)
        # Nome do arquivo HTML
        html_file = f"output_{file_id}.html"
    elif database_type == 'mysql':
        # Supondo que 'rtf_text' seja uma string
        # Abra o arquivo em modo de escrita binária ('wb') para aceitar objetos de bytes
        with open(rtf_file, 'wb') as file:
            # Converta a string em bytes usando a codificação 'utf-8'
            file.write(rtf_text.encode('utf-8'))
        # Nome do arquivo HTML
        html_file = f"output_{file_id}.html"

    try:
        # Converter o arquivo RTF para HTML
        convert_rtf_to_html(rtf_file, html_file)

        # Ler o conteúdo HTML convertido
        with open(html_file, "r", encoding="utf-8") as file:
            html_text = file.read()

        # Remover a tag <img> e seu conteúdo
        soup = BeautifulSoup(html_text, "html.parser")
        for img_tag in soup.find_all("img"):
            img_tag.extract()

        for table in soup.find_all('table'):
            table.extract()

        for tag in soup.find_all(True):
            tag.attrs = {key: value for key, value in tag.attrs.items() if key != 'style'}

        for tag in soup.find_all(True):
            if tag.string and tag.string.strip() == '':
                tag.extract()

        # Realiza o replace no html
        html_text = str(soup)
        html_text = html_text.replace("Evaluation Only. Created with Aspose.Words. Copyright 2003-2023 Aspose Pty Ltd.","")
        html_text = html_text.replace("Created with an evaluation copy of Aspose.Words. To discover the full versions of our APIs please visit: https://products.aspose.com/words/","")
        html_text = html_text.replace("<title></title></head><body><div><div><p><span></span></p><p></p></div><p><span></span></p><p>", "")
        html_text = html_text.replace('<meta content="Aspose.Words for Python via .NET 23.4.0" name="generator"/></p><p><br/></p><p>', '')
        html_text = html_text.replace("Nome:", "")
        html_text = html_text.replace("Registro:", "")
        html_text = html_text.replace("Convênio:", "")
        html_text = html_text.replace("Idade:", "")
        html_text = html_text.replace("Médico:", "")
        html_text = html_text.replace("Data:", "")

        # Atribuir o conteúdo HTML diretamente à variável body_content
        body_content = html_text

        # Inserir o conteúdo HTML, o ID e a data/hora de importação na tabela de saída
        insert_query = f"INSERT INTO {output_table} (id_laudo, texto, dt_import_laudo, convertido) VALUES (%s, %s, %s, %s)"
        cursor.execute(insert_query, (file_id, body_content, current_datetime, True))
        conn.commit()

        # Registrar as informações relevantes no log
        logging.info(f"ID Laudo: {file_id} importado com sucesso")

        # Atualizar o último id_laudo lido no seu serviço
        ultimo_id_laudo_lido = file_id

        # Remover os arquivos temporários
        os.remove(rtf_file)
        os.remove(html_file)

        # Remover arquivos PNG e JPEG
        for filename in os.listdir('.'):
            if filename.endswith(('.png', '.jpeg', '.jpg')):
                os.remove(filename)

    # Registrar eventuais erros no log
    except Exception as e:
        logging.error(f"Erro ao converter o arquivo ID: {file_id} - {str(e)}")

# Fechar a conexão
cursor.close()
conn.close()
# Finalização da conversão no log
logging.info("Conversão concluída. Os arquivos RTF foram convertidos para HTML e salvos na tabela do " + (database_type))

