import os
import datetime
import aspose.words as aw
from bs4 import BeautifulSoup
from connect import *

def convert_rtf_to_html(rtf_file, html_file):
    doc = aw.Document(rtf_file)
    doc.save(html_file)

# Utilização da função
conn, cursor, connected = connect_to_database()

if connected:
    print("Conexão com o banco de dados estabelecida com sucesso!")
    # Restante do código para realizar operações no banco de dados
else:
    print("Falha ao conectar ao banco de dados.")
    # Lidar com a falha na conexão

cursor = conn.cursor()

# Definir a tabela de entrada e a tabela de saída
input_table = "legacy.laudo_legado"
output_table = "legacy.laudo_import_html"

# Criar a tabela de saída se ela não existir
create_table_query = f"""
    CREATE TABLE IF NOT EXISTS legacy.laudo_import_html (
        id SERIAL PRIMARY KEY,
        id_laudo VARCHAR,
        texto TEXT,
        erro TEXT,
        dt_import_laudo TIMESTAMP
    )
"""
cursor.execute(create_table_query)
conn.commit()


# Adicionar um índice na coluna "id_laudo" da tabela de saída
add_index_query = f"CREATE INDEX IF NOT EXISTS idx_id_laudo ON {output_table} (id_laudo)"
cursor.execute(add_index_query)
conn.commit()

# Ler os dados da tabela de entrada
select_query = f"SELECT idlaudo, laudo FROM {input_table}"
cursor.execute(select_query)
rows = cursor.fetchall()

# Obter a data e hora atual
current_datetime = datetime.datetime.now()

# Converter cada arquivo RTF para HTML e salvar na tabela de saída
for row in rows:
    file_id, rtf_text = row
    rtf_file = f"input_{file_id}.rtf"
    html_file = f"output_{file_id}.html"

    # Verificar se o ID é nulo/vazio
    if file_id is None or file_id == '':
        print(f"Arquivo ID: {file_id} Laudo já convertido ou ID nulo/vazio, passando para o próximo.")
        # Inserir o erro na coluna "erro" da tabela de saída
        insert_error_query = f"INSERT INTO {output_table} (id_laudo, erro, dt_import_laudo) VALUES (%s, %s, %s)"
        cursor.execute(insert_error_query, (None, "ID é nulo ou vazio", current_datetime))
        conn.commit()
        print(f"ID é nulo ou vazio")
        continue

    # Verificar se o ID já foi convertido
    check_query = f"SELECT id_laudo FROM laudo_import_html WHERE id_laudo = %s"
    cursor.execute(check_query, (file_id,))
    result = cursor.fetchone()
    if result:
        # Inserir na coluna erro se o laudo já foi convertido
        insert_error_query = f"INSERT INTO laudo_import_html (id_laudo, erro, dt_import_laudo) VALUES (%s, %s, %s)"
        cursor.execute(insert_error_query, (file_id, f"Arquivo ID: {file_id} Laudo já convertido, passando para o próximo.", current_datetime))
        conn.commit()
        print(f"Arquivo ID: {file_id} Laudo já convertido, passando para o próximo.")
        continue

     # Salvar o conteúdo do RTF em um arquivo temporário
    with open(rtf_file, "wb") as file:
        file.write(rtf_text)

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

        # Realizar o replace no texto
        html_text = str(soup)
        html_text = html_text.replace("Evaluation Only. Created with Aspose.Words. Copyright 2003-2023 Aspose Pty Ltd.", "")
        html_text = html_text.replace("Created with an evaluation copy of Aspose.Words. To discover the full versions of our APIs please visit: https://products.aspose.com/words/", "")
        html_text = html_text.replace("<title></title></head><body><div><div><p><span></span></p><p></p></div><p><span></span></p><p>", "")
        html_text = html_text.replace('<meta content="Aspose.Words for Python via .NET 23.4.0" name="generator"/></p><p><br/></p><p>', '')
        html_text = html_text.replace("Nome:","")
        html_text = html_text.replace("Registro:","")
        html_text = html_text.replace("Convênio:","")
        html_text = html_text.replace("Idade:","")
        html_text = html_text.replace("Médico:","")
        html_text = html_text.replace("Data:","")
        
        # Atribuir o conteúdo HTML diretamente à variável body_content
        body_content = html_text

        # Inserir o conteúdo HTML, o ID e a data/hora de importação na tabela de saída
        insert_query = f"INSERT INTO {output_table} (id_laudo, texto, dt_import_laudo) VALUES (%s, %s, %s)"
        cursor.execute(insert_query, (file_id, body_content, current_datetime))
        conn.commit()

        print(f"Arquivo ID: {file_id} convertido com sucesso.")

        # Remover os arquivos temporários
        os.remove(rtf_file)
        os.remove(html_file)

        # Remover arquivos PNG e JPEG
        for filename in os.listdir('.'):
            if filename.endswith(('.png', '.jpeg', '.jpg')):
                os.remove(filename)
                
    except Exception as e:
        print(f"Erro ao converter o arquivo ID: {file_id} - {str(e)}")

# Fechar a conexão
cursor.close()
conn.close()

print("Conversão concluída. Os arquivos RTF foram convertidos para HTML e salvos na tabela do PostgreSQL.")