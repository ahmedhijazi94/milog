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
        # Criação da tabela para as empresas (wpxx_empresas_livelo)
        create_empresas_table_query = """
        CREATE TABLE IF NOT EXISTS wpxx_empresas_livelo (
            id INT AUTO_INCREMENT PRIMARY KEY,
            nome VARCHAR(255) UNIQUE NOT NULL,
            logo VARCHAR(255)
        );
        """
        cursor.execute(create_empresas_table_query)

        # Criação da tabela de pontuação (wpxx_livelo_pontuacao) com a nova coluna descricao_text
        create_pontuacao_table_query = """
        CREATE TABLE IF NOT EXISTS wpxx_livelo_pontuacao (
            id INT AUTO_INCREMENT PRIMARY KEY,
            data_hora_coleta DATETIME NOT NULL,
            moeda VARCHAR(10),
            pontuacao FLOAT,
            pontuacao_clube_livelo FLOAT,
            empresa_id INT,
            descricao_text TEXT,
            FOREIGN KEY (empresa_id) REFERENCES wpxx_empresas_livelo(id)
        );
        """
        cursor.execute(create_pontuacao_table_query)
        connection.commit()
        print("[INFO] Tabelas 'wpxx_empresas_livelo' e 'wpxx_livelo_pontuacao' criadas ou já existentes.")
    except mysql.connector.Error as err:
        print(f"[ERROR] Não foi possível criar as tabelas: {err}")


def obter_empresa_id(nome_empresa, logo, connection):
    """
    Verifica se a empresa já está cadastrada. Se sim, atualiza o logo,
    caso contrário, insere a empresa e retorna o novo ID.
    """
    cursor = connection.cursor()
    cursor.execute("SELECT id, logo FROM wpxx_empresas_livelo WHERE nome = %s", (nome_empresa,))
    empresa = cursor.fetchone()

    if empresa:
        # Empresa já existe, vamos atualizar o logo se for diferente
        empresa_id, current_logo = empresa
        if current_logo != logo:
            cursor.execute("UPDATE wpxx_empresas_livelo SET logo = %s WHERE id = %s", (logo, empresa_id))
            connection.commit()
            print(f"[INFO] Logo atualizado para a empresa '{nome_empresa}'.")
        return empresa_id
    else:
        # Inserir nova empresa com o logo
        cursor.execute("INSERT INTO wpxx_empresas_livelo (nome, logo) VALUES (%s, %s)", (nome_empresa, logo))
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
    Acessa a página da Livelo, coleta as informações dos cards de parceiros
    e retorna uma lista de dicionários com:
      - nome
      - moeda
      - descricao_text
      - pontuacao
      - pontuacao_clube_livelo
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
        WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler"))
        ).click()
        print("[INFO] Cookies aceitos.")
    except:
        print("[INFO] Nenhum pop-up de cookies encontrado.")

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
        
        # Extraindo o link da imagem e adicionando o prefixo necessário
        logo_relativo = img_tag.get("src", "") if img_tag else ""
        logo_completo = f"{logo_relativo}" if logo_relativo else ""

        descricao_principal = ""
        info_value = card.find("div", class_="info__value")
        if info_value:
            descricao_principal = info_value.get_text(" ", strip=True)

        clube_livelo = card.find("div", class_="info__club")
        texto_clube_livelo = clube_livelo.get_text(" ", strip=True) if clube_livelo else ""

        if texto_clube_livelo:
            descricao_principal = descricao_principal.lstrip("ou até ").strip()
            descricao_completa = f"{texto_clube_livelo} no Clube Livelo ou até {descricao_principal}"
        else:
            descricao_completa = descricao_principal

        moeda, pontuacao, pontuacao_clube = parse_descricao(descricao_completa)

        empresa_id = obter_empresa_id(nome, logo_completo, connection)

        parceiros.append({
            "empresa_id": empresa_id,
            "moeda": moeda,
            "pontuacao": pontuacao,
            "pontuacao_clube_livelo": pontuacao_clube,
            "descricao_text": descricao_completa
        })

    return parceiros


def salvar_relatorio_mysql(parceiros, connection):
    """
    Insere os dados de pontuação no banco de dados MySQL.
    Relacionando com a empresa e agora incluindo a descrição do parceiro.
    """
    if not parceiros:
        print("[WARN] Lista de parceiros vazia; não há o que salvar.")
        return

    try:
        cursor = connection.cursor()

        for parceiro in parceiros:
            insert_query = """
            INSERT INTO wpxx_livelo_pontuacao (
                data_hora_coleta, moeda, pontuacao, pontuacao_clube_livelo, empresa_id, descricao_text
            ) VALUES (%s, %s, %s, %s, %s, %s)
            """
            data_hora_coleta = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            cursor.execute(insert_query, (
                data_hora_coleta,
                parceiro["moeda"],
                parceiro["pontuacao"],
                parceiro["pontuacao_clube_livelo"],
                parceiro["empresa_id"],
                parceiro["descricao_text"]  # Adiciona a descrição do parceiro
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
