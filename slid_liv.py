import os  # ✅ Importação corrigida
import mysql.connector
import time
from datetime import datetime
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import logging
import json
import re

# Configuração do logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_env_var(var_name: str) -> str:
    """
    Lê a variável de ambiente 'var_name'.
    Se não estiver definida, gera um erro (ValueError).
    """
    value = os.getenv(var_name)
    if not value:
        raise ValueError(f"A variável de ambiente '{var_name}' não está definida!")
    return value

def conectar_banco():
    """
    Conecta ao banco de dados MySQL e retorna o objeto de conexão.
    """
    try:
        connection = mysql.connector.connect(
            host=os.getenv("DB_HOST"),        # Host do banco de dados
            database=os.getenv("DB_NAME"),    # Nome do banco de dados
            user=os.getenv("DB_USER"),        # Usuário do banco de dados
            password=os.getenv("DB_PASSWORD") # Senha do banco de dados
        )
        if connection.is_connected():
            logging.info("Conectado ao banco de dados.")
            return connection
    except mysql.connector.Error as err:
        logging.error(f"Não foi possível conectar ao banco de dados: {err}")
        return None

def criar_tabela_banners(connection):
    """
    Cria a tabela no banco de dados caso ela não exista.
    O nome da tabela é lido obrigatoriamente da variável de ambiente.
    """
    # Lê o nome da tabela (sem fallback)
    table_banners = get_env_var("TABLE_BANNERS_LIV")

    try:
        cursor = connection.cursor()
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_banners} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            datahora_coleta DATETIME NOT NULL,
            banners JSON NOT NULL
        );
        """
        cursor.execute(create_table_query)
        connection.commit()
        logging.info(f"Tabela '{table_banners}' criada ou já existente.")
    except mysql.connector.Error as err:
        logging.error(f"Erro ao criar a tabela: {err}")

def extrair_banners():
    """
    Acessa a página principal da Livelo, extrai todos os textos dos banners,
    incluindo títulos, subtítulos, textos adicionais, e links de redirecionamento,
    retornando uma lista de dicionários.
    """
    url = "https://www.livelo.com.br/"

    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Executa o Chrome em modo headless
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
    )

    # Inicializa o WebDriver
    driver = webdriver.Chrome(options=chrome_options)
    driver.set_window_size(1920, 1080)

    logging.info("Abrindo página principal da Livelo...")
    driver.get(url)

    try:
        # Aceitar cookies se o botão estiver presente
        WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler"))
        ).click()
        logging.info("Cookies aceitos.")
    except Exception:
        logging.info("Nenhum pop-up de cookies encontrado ou já aceito.")

    try:
        # Espera até que o slider esteja presente
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.owl-stage-outer.banner--large-default"))
        )
        logging.info("Slider encontrado.")
    except Exception as e:
        logging.error(f"Timeout ao esperar o slider: {e}")
        driver.quit()
        return []

    # Aguarda um pouco para garantir que todos os elementos sejam carregados
    time.sleep(2)

    html = driver.page_source
    driver.quit()

    soup = BeautifulSoup(html, "html.parser")
    slider_div = soup.find("div", class_="owl-stage-outer banner--large-default")
    if not slider_div:
        logging.error("Não foi possível encontrar a div do slider.")
        return []

    # Encontra todos os itens do slider
    owl_items = slider_div.find_all("div", class_="owl-item")
    logging.info(f"Total de banners encontrados: {len(owl_items)}")

    banners = []
    for idx, item in enumerate(owl_items, start=1):
        # Cada 'owl-item' contém um banner
        banner_div = item.find("div", class_="div-banner")
        if not banner_div:
            logging.warning(f"BANNER {idx}: div-banner não encontrado.")
            continue

        # Extrai todos os textos do banner
        texts = []

        # Extrai títulos (h1, h2, h3)
        title_tags = banner_div.find_all(["h1", "h2", "h3"])
        for tag in title_tags:
            text = tag.get_text(strip=True)
            if text:
                texts.append(text)

        # Extrai spans com classes que começem com 'text--'
        span_tags = banner_div.find_all("span", class_=re.compile(r'^text--'))
        for span in span_tags:
            text = span.get_text(strip=True)
            if text:
                texts.append(text)

        # Extrai parágrafos
        p_tags = banner_div.find_all("p")
        for p in p_tags:
            text = p.get_text(strip=True)
            if text:
                texts.append(text)

        # Link de redirecionamento
        button = banner_div.find("button", class_=re.compile(r'banner-carousel-button'))
        redirect_link = ""
        if button:
            onclick_attr = button.get("onclick", "")
            match = re.search(r"window\.location\.href=['\"](.*?)['\"]", onclick_attr)
            if match:
                redirect_link = match.group(1)
            else:
                # Alternativamente, extrair de 'data-gtm-event-label'
                redirect_link = button.get("data-gtm-event-label", "")
                if not redirect_link:
                    # Tentar extrair de 'data-gtm-event-action'
                    redirect_link = button.get("data-gtm-event-action", "")

        banner_data = {
            "texts": texts,  # lista de todos os textos extraídos
            "redirect_link": redirect_link
        }

        # Validação: pelo menos um texto ou link precisa existir
        if texts or redirect_link:
            banners.append(banner_data)
            logging.info(f"BANNER {idx}: Texto extraído com sucesso.")
        else:
            logging.warning(f"BANNER {idx}: Nenhum texto ou link encontrado.")

    logging.info(f"Total de banners válidos extraídos: {len(banners)}")
    return banners

def salvar_banners_mysql(banners, connection):
    """
    Insere os dados de banners no banco de dados MySQL.
    """
    if not banners:
        logging.warning("Lista de banners vazia; não há o que salvar.")
        return

    # Lê o nome da tabela de banners (sem fallback)
    table_banners = get_env_var("TABLE_BANNERS_LIV")

    try:
        cursor = connection.cursor()
        insert_query = f"""
        INSERT INTO {table_banners} (datahora_coleta, banners)
        VALUES (%s, %s)
        """
        data_hora_coleta = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        banners_json = json.dumps(banners, ensure_ascii=False)  # Garante que caracteres especiais sejam mantidos

        cursor.execute(insert_query, (
            data_hora_coleta,
            banners_json
        ))
        connection.commit()
        logging.info("Banners inseridos no banco de dados com sucesso.")
    except mysql.connector.Error as err:
        logging.error(f"Erro ao inserir banners no banco de dados: {err}")

def main():
    connection = conectar_banco()
    if connection:
        criar_tabela_banners(connection)
        banners = extrair_banners()
        if banners:
            salvar_banners_mysql(banners, connection)
        connection.close()
    else:
        logging.error("Falha na conexão com o banco de dados. O bot será encerrado.")

if __name__ == "__main__":
    main()
