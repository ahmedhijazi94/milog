import os  # ✅ Importação corrigida
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
            host=os.getenv("DB_HOST"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD")
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

        table_empresas = os.getenv("TABLE_EMPRESAS_LIV")
        table_pontuacao = os.getenv("TABLE_PONTUACAO_LIV")

        create_empresas_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_empresas} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            nome VARCHAR(255) UNIQUE NOT NULL,
            logo VARCHAR(255)
        );
        """
        cursor.execute(create_empresas_table_query)

        create_pontuacao_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_pontuacao} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            data_hora_coleta DATETIME NOT NULL,
            moeda VARCHAR(10),
            pontuacao FLOAT,
            pontuacao_clube_livelo FLOAT,
            empresa_id INT,
            descricao_text TEXT,
            FOREIGN KEY (empresa_id) REFERENCES {table_empresas}(id)
        );
        """
        cursor.execute(create_pontuacao_table_query)

        connection.commit()
        print(f"[INFO] Tabelas '{table_empresas}' e '{table_pontuacao}' criadas ou já existentes.")
    except mysql.connector.Error as err:
        print(f"[ERROR] Não foi possível criar as tabelas: {err}")


def obter_empresa_id(nome_empresa, logo, connection):
    """
    Verifica se a empresa já está cadastrada. Se sim, atualiza o logo,
    caso contrário, insere a empresa e retorna o novo ID.
    """
    cursor = connection.cursor()
    table_empresas = os.getenv("TABLE_EMPRESAS_LIV")

    cursor.execute(f"SELECT id, logo FROM {table_empresas} WHERE nome = %s", (nome_empresa,))
    empresa = cursor.fetchone()

    if empresa:
        empresa_id, current_logo = empresa
        if current_logo != logo:
            cursor.execute(f"UPDATE {table_empresas} SET logo = %s WHERE id = %s", (logo, empresa_id))
            connection.commit()
            print(f"[INFO] Logo atualizado para a empresa '{nome_empresa}'.")
        return empresa_id
    else:
        cursor.execute(f"INSERT INTO {table_empresas} (nome, logo) VALUES (%s, %s)", (nome_empresa, logo))
        connection.commit()
        print(f"[INFO] Empresa '{nome_empresa}' inserida com sucesso.")
        return cursor.lastrowid


def parse_descricao(descricao: str):
    """
    Faz o parse do texto para identificar:
      - Moeda (R$ ou U$)
      - Valor base do dinheiro (p.ex.: 1, 2, etc.)
      - Pontuação 'normal' (pontuacao)
      - Pontuação 'clube' (pontuacao_clube)
    """
    moeda = ""
    base_money = 1.0
    pontuacao = "x"
    pontuacao_clube = "x"

    match_moeda = re.search(r"(R\$|U\$)\s*(\d+(?:,\d+)?)", descricao)
    if match_moeda:
        moeda = match_moeda.group(1)
        base_money_str = match_moeda.group(2).replace(",", ".")
        base_money = float(base_money_str)

    match_clube = re.search(r"(?:até|=)\s*(\d+(?:,\d+)?)(?=.*no Clube Livelo)", descricao, re.IGNORECASE)
    if match_clube:
        valor_str = match_clube.group(1).replace(",", ".")
        valor_num = float(valor_str)
        ratio = valor_num / base_money
        pontuacao_clube = round(ratio, 3)

    match_normal = re.search(r"(?:até|=)\s*(\d+(?:,\d+)?)(?=.*Pontos?\s+Livelo)(?!.*no Clube)", 
                             descricao, re.IGNORECASE)
    if match_normal:
        valor_str = match_normal.group(1).replace(",", ".")
        valor_num = float(valor_str)
        ratio = valor_num / base_money
        pontuacao = round(ratio, 3)

    return moeda, pontuacao, pontuacao_clube


def extrair_parceiros(connection):
    """
    Acessa a página da Livelo, coleta as informações dos cards de parceiros.
    """
    url = "https://www.livelo.com.br/ganhe-pontos-compre-e-pontue"

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

    try:
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.parity__card"))
        )
        print("[INFO] Cards encontrados.")
    except:
        print("[ERROR] Timeout ao esperar os cards.")
        driver.quit()
        return []

    time.sleep(2)
    html = driver.page_source
    driver.quit()

    soup = BeautifulSoup(html, "html.parser")
    div_cards = soup.find("div", id="div-cardsParity")
    if not div_cards:
        print("[ERROR] Não foi possível encontrar a div com os cards.")
        return []

    cards = div_cards.find_all("div", class_="parity__card")
    print(f"[INFO] Total de cards encontrados: {len(cards)}")

    parceiros = []
    for card in cards:
        img_tag = card.find("img", class_="parity__card--img")
        nome = img_tag.get("alt", "Nome não encontrado") if img_tag else "Nome não encontrado"
        logo = img_tag.get("src", "") if img_tag else ""

        descricao = card.find("div", class_="info__value")
        descricao_text = descricao.get_text(" ", strip=True) if descricao else "Descrição não encontrada"

        moeda, pontuacao, pontuacao_clube = parse_descricao(descricao_text)
        empresa_id = obter_empresa_id(nome, logo, connection)

        parceiros.append({
            "empresa_id": empresa_id,
            "moeda": moeda,
            "pontuacao": pontuacao,
            "pontuacao_clube_livelo": pontuacao_clube,
            "descricao_text": descricao_text
        })

    return parceiros


def salvar_relatorio_mysql(parceiros, connection):
    """
    Insere os dados de pontuação no banco de dados MySQL.
    """
    if not parceiros:
        print("[WARN] Lista de parceiros vazia; não há o que salvar.")
        return

    try:
        cursor = connection.cursor()
        table_pontuacao = os.getenv("TABLE_PONTUACAO_LIV")

        for parceiro in parceiros:
            insert_query = f"""
            INSERT INTO {table_pontuacao} (
                data_hora_coleta, moeda, pontuacao, pontuacao_clube_livelo, empresa_id, descricao_text
            ) VALUES (%s, %s, %s, %s, %s, %s)
            """
            data_hora_coleta = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            cursor.execute(insert_query, (
                data_hora_coleta, parceiro["moeda"], parceiro["pontuacao"],
                parceiro["pontuacao_clube_livelo"], parceiro["empresa_id"], parceiro["descricao_text"]
            ))

        connection.commit()
        print("[INFO] Dados inseridos com sucesso.")
    except mysql.connector.Error as err:
        print(f"[ERROR] Erro ao inserir dados no banco: {err}")


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
