import mysql.connector
import re
import time
from datetime import datetime
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


def conectar_banco():
    """
    Conecta ao banco de dados MySQL e retorna o objeto de conexão.
    """
    try:
        connection = mysql.connector.connect(
            host='162.241.60.233',  # Exemplo: 'localhost' ou o IP do servidor
            database='milhas15_wp271',  # Nome do banco de dados
            user='milhas15_user_bot',  # Nome de usuário
            password='Hijazi564.'  # Senha fornecida
        )
        if connection.is_connected():
            print("[INFO] Conectado ao banco de dados.")
            return connection
    except mysql.connector.Error as err:
        print(f"[ERROR] Não foi possível conectar ao banco de dados: {err}")
        return None


def criar_tabelas(connection):
    """
    Cria as tabelas no banco de dados caso elas não existam.
    """
    try:
        cursor = connection.cursor()
        # Criação da tabela para as empresas (wpxx_empresas_esfera)
        create_empresas_table_query = """
        CREATE TABLE IF NOT EXISTS wpxx_empresas_esfera (
            id INT AUTO_INCREMENT PRIMARY KEY,
            nome VARCHAR(255) UNIQUE NOT NULL,
            logo VARCHAR(255)
        );
        """
        cursor.execute(create_empresas_table_query)

        # Criação da tabela de pontuação (wpxx_esfera_pontuacao)
        create_pontuacao_table_query = """
        CREATE TABLE IF NOT EXISTS wpxx_esfera_pontuacao (
            id INT AUTO_INCREMENT PRIMARY KEY,
            data_hora_coleta DATETIME NOT NULL,
            moeda VARCHAR(10),
            pontuacao FLOAT,
            descricao_text TEXT,
            empresa_id INT,
            FOREIGN KEY (empresa_id) REFERENCES wpxx_empresas_esfera(id)
        );
        """
        cursor.execute(create_pontuacao_table_query)
        connection.commit()
        print("[INFO] Tabelas 'wpxx_empresas_esfera' e 'wpxx_esfera_pontuacao' criadas ou já existentes.")
    except mysql.connector.Error as err:
        print(f"[ERROR] Não foi possível criar as tabelas: {err}")


def obter_empresa_id(nome_empresa, logo, connection):
    """
    Verifica se a empresa já está cadastrada. Se sim, atualiza o logo,
    caso contrário, insere a empresa e retorna o novo ID.
    """
    cursor = connection.cursor()
    cursor.execute("SELECT id, logo FROM wpxx_empresas_esfera WHERE nome = %s", (nome_empresa,))
    empresa = cursor.fetchone()

    if empresa:
        # Empresa já existe, vamos atualizar o logo se for diferente
        empresa_id, current_logo = empresa
        if current_logo != logo:
            cursor.execute("UPDATE wpxx_empresas_esfera SET logo = %s WHERE id = %s", (logo, empresa_id))
            connection.commit()
            print(f"[INFO] Logo atualizado para a empresa '{nome_empresa}'.")
        return empresa_id
    else:
        # Inserir nova empresa com o logo
        cursor.execute("INSERT INTO wpxx_empresas_esfera (nome, logo) VALUES (%s, %s)", (nome_empresa, logo))
        connection.commit()
        print(f"[INFO] Empresa '{nome_empresa}' inserida com sucesso.")
        return cursor.lastrowid


def extrair_pontuacao(descricao: str):
    """
    Faz o parse da descrição para identificar a pontuação e a moeda associada:
      - Moeda (R$, U$, Eu$)
      - Pontuação associada (x ou valores numéricos)
    """
    moeda = "R$"  # Valor fixo de moeda, se não for encontrado nenhum outro
    pontuacao = "x"  # Pontuação padrão

    # Identificar moeda
    if "real" in descricao.lower():  # Detecta "real" ou "reais"
        moeda = "R$"
    elif "dólar" in descricao.lower():  # Detecta "dólar"
        moeda = "U$"
    elif "euro" in descricao.lower():  # Detecta "euro"
        moeda = "Eu$"

    # Regra especial: "a cada x reais"
    if "a cada" in descricao.lower() and "reais" in descricao.lower():
        numerador = re.search(r'(\d+,\d+|\d+)\s?pt', descricao)  # Número antes de "pt"
        denominador = re.search(r'(\d+,\d+|\d+)\s?reais', descricao)  # Número antes de "reais"
        if numerador and denominador:
            # Dividir o numerador pelo denominador
            numerador_value = numerador.group(1).replace(',', '.')
            denominador_value = denominador.group(1).replace(',', '.')
            # Calcular a pontuação
            try:
                divisao = float(numerador_value) / float(denominador_value)
                return moeda, str(divisao)  # Retorna o valor da divisão
            except ZeroDivisionError:
                return moeda, "0"  # Caso o denominador seja 0, retorna 0
    
    # Regra 1: "Ganhe de x a x pts" -> maior valor
    if "de" in descricao.lower() and "a" in descricao.lower():
        valores = re.findall(r'\d+,\d+|\d+', descricao)
        if valores:
            return moeda, max(valores, key=lambda x: float(x.replace(',', '.')))

    # Regra geral: número antes de "pt" ou "pts"
    pontuacao_match = re.search(r'(\d+,\d+|\d+)\s?(pt|pts)', descricao)
    if pontuacao_match:
        return moeda, pontuacao_match.group(1)

    return moeda, pontuacao


def extrair_parceiros(connection):
    """
    Acessa a página da Esfera, coleta as informações dos cards de parceiros
    e retorna uma lista de dicionários com:
      - nome
      - moeda
      - descricao_text
      - logo
      - pontuacao
    """
    url = "https://www.esfera.com.vc/c/ganhe-pontos/esf02163"

    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
    )

    driver = webdriver.Chrome(options=chrome_options)
    driver.set_window_size(1920, 1080)

    print("[INFO] Abrindo página...")
    driver.get(url)

    # Aguardar os cards carregarem
    try:
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CLASS_NAME, "box-partner-custom"))
        )
        print("[INFO] Cards encontrados.")
    except:
        print("[ERROR] Timeout ao esperar os cards.")
        driver.quit()
        return []

    # Capturar o HTML
    time.sleep(2)  # garantir carregamento final
    html = driver.page_source
    driver.quit()

    # Parsear o HTML com BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    div_cards = soup.find_all("div", class_="col-xs-6 col-sm-3 col-lg-2")
    if not div_cards:
        print("[ERROR] Não foi possível encontrar os cards.")
        return []

    print(f"[INFO] Total de cards encontrados: {len(div_cards)}")

    parceiros = []
    for card in div_cards:
        # Nome da empresa a partir do texto dentro da div -partnerName
        nome = card.find("div", class_="-partnerName")
        nome = nome.get_text(strip=True) if nome else "Nome não encontrado"

        # Imagem da logo do parceiro
        img_tag = card.find("img")
        logo = img_tag.get("src", "Logo não encontrada") if img_tag else "Logo não encontrada"

        # Pontuação e descrição
        descricao = card.find("div", class_="-partnerPoints")
        descricao_text = descricao.get_text(" ", strip=True) if descricao else "Descrição não encontrada"

        moeda, pontuacao = extrair_pontuacao(descricao_text)

        empresa_id = obter_empresa_id(nome, logo, connection)

        parceiros.append({
            "empresa_id": empresa_id,
            "moeda": moeda,
            "pontuacao": pontuacao,
            "descricao_text": descricao_text
        })

    return parceiros


def salvar_relatorio_mysql(parceiros, connection):
    """
    Insere os dados de pontuação no banco de dados MySQL.
    Relacionando com a empresa.
    """
    if not parceiros:
        print("[WARN] Lista de parceiros vazia; não há o que salvar.")
        return

    try:
        cursor = connection.cursor()

        for parceiro in parceiros:
            insert_query = """
            INSERT INTO wpxx_esfera_pontuacao (
                data_hora_coleta, moeda, pontuacao, descricao_text, empresa_id
            ) VALUES (%s, %s, %s, %s, %s)
            """
            data_hora_coleta = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            cursor.execute(insert_query, (
                data_hora_coleta,
                parceiro["moeda"],
                parceiro["pontuacao"],
                parceiro["descricao_text"],
                parceiro["empresa_id"]
            ))

        connection.commit()
        print("[INFO] Dados inseridos no banco de dados com sucesso.")
    except mysql.connector.Error as err:
        print(f"[ERROR] Erro ao inserir dados no banco de dados: {err}")


def main():
    connection = conectar_banco()
    if connection:
        criar_tabelas(connection)
        parceiros = extrair_parceiros(connection)
        if parceiros:
            salvar_relatorio_mysql(parceiros, connection)
        connection.close()

if __name__ == "__main__":
    main()
