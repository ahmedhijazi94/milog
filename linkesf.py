import os
import mysql.connector
import time
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from contextlib import contextmanager
import logging
from dotenv import load_dotenv

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("bot_esfera.log"),
        logging.StreamHandler()
    ]
)

# Constantes
URL_ESFERA = "https://www.esfera.com.vc/c/ganhe-pontos/esf02163"
SELECTOR_CARDS = "div.col-xs-6.col-sm-3.col-lg-2"
SELECTOR_BOX_PARTNER = "div.box-partner-custom"
SELECTOR_LINK = "a"  # Seletores para o <a> dentro do card
SELECTOR_ACCEPT_COOKIES = "button#onetrust-accept-btn-handler"  # Atualize conforme a página

@contextmanager
def obter_conexao():
    """
    Context manager para obter uma conexão com o banco de dados MySQL.
    Garante que a conexão seja fechada adequadamente.
    """
    connection = None
    try:
        connection = mysql.connector.connect(
            host=os.getenv("DB_HOST"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD")
        )
        if connection.is_connected():
            logging.info("Conectado ao banco de dados.")
            yield connection
    except mysql.connector.Error as err:
        logging.error(f"Não foi possível conectar ao banco de dados: {err}")
        yield None
    finally:
        if connection and connection.is_connected():
            connection.close()
            logging.info("Conexão com o banco de dados fechada.")

def get_env_var(var_name: str) -> str:
    """
    Lê a variável de ambiente 'var_name'.
    Se não estiver definida, gera um erro (ValueError).
    """
    value = os.getenv(var_name)
    if not value:
        raise ValueError(f"A variável de ambiente '{var_name}' não está definida!")
    return value

def obter_empresa_id(nome_empresa, logo, connection):
    """
    Verifica se a empresa já está cadastrada. Se sim, atualiza o logo,
    caso contrário, insere a empresa e retorna o novo ID.
    """
    table_empresas = get_env_var("TABLE_EMPRESAS_ESF")

    try:
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT id, logo FROM {table_empresas} WHERE nome = %s", (nome_empresa,))
            empresa = cursor.fetchone()

            if empresa:
                empresa_id, current_logo = empresa
                if current_logo != logo:
                    cursor.execute(f"UPDATE {table_empresas} SET logo = %s WHERE id = %s", (logo, empresa_id))
                    connection.commit()
                    logging.info(f"Logo atualizado para a empresa '{nome_empresa}'.")
                return empresa_id
            else:
                cursor.execute(f"INSERT INTO {table_empresas} (nome, logo) VALUES (%s, %s)", (nome_empresa, logo))
                connection.commit()
                logging.info(f"Empresa '{nome_empresa}' inserida com sucesso.")
                return cursor.lastrowid
    except mysql.connector.Error as err:
        logging.error(f"Erro ao obter ou inserir empresa '{nome_empresa}': {err}")
        return None

def configurar_selenium():
    """
    Configura e retorna o WebDriver do Selenium com as opções necessárias.
    """
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")  # Atualizado para headless novo se disponível
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
    )

    try:
        driver = webdriver.Chrome(options=chrome_options)
        driver.set_window_size(1920, 1080)
        logging.info("WebDriver do Selenium iniciado com sucesso.")
        return driver
    except Exception as e:
        logging.error(f"Não foi possível iniciar o WebDriver do Selenium: {e}")
        return None

def extrair_parceiros(driver, connection):
    """
    Acessa a página da Esfera, coleta as informações dos cards de parceiros
    e retorna uma lista de dicionários com:
      - nome
      - logo
      - empresa_id
    """
    parceiros = []

    try:
        logging.info("Abrindo página da Esfera...")
        driver.get(URL_ESFERA)

        # Aceitar cookies se o botão estiver presente
        try:
            WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, SELECTOR_ACCEPT_COOKIES))
            ).click()
            logging.info("Cookies aceitos.")
        except Exception:
            logging.info("Nenhum pop-up de cookies encontrado ou já aceito.")

        # Aguardar os cards carregarem
        try:
            WebDriverWait(driver, 20).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, SELECTOR_CARDS))
            )
            logging.info("Cards encontrados.")
        except:
            logging.error("Timeout ao esperar os cards.")
            return parceiros

        # Capturar o HTML
        time.sleep(2)  # Garantir carregamento final
        html = driver.page_source

        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "html.parser")
        div_cards = soup.select(SELECTOR_CARDS)
        if not div_cards:
            logging.error("Não foi possível encontrar os cards.")
            return parceiros

        logging.info(f"Total de cards encontrados: {len(div_cards)}")

        for index, card in enumerate(div_cards, start=1):
            try:
                # Nome da empresa
                nome_div = card.find("div", class_="-partnerName")
                nome = nome_div.get_text(strip=True) if nome_div else "Nome não encontrado"

                # Imagem da logo
                img_tag = card.find("img")
                logo = img_tag.get("src", "Logo não encontrada") if img_tag else "Logo não encontrada"

                empresa_id = obter_empresa_id(nome, logo, connection)
                if not empresa_id:
                    logging.warning(f"Não foi possível obter o ID para a empresa '{nome}'. Pulando.")
                    continue

                parceiros.append({
                    "empresa_id": empresa_id,
                    "nome": nome,
                    "logo": logo,
                    "card_index": index  # Índice para referência
                })

                logging.info(f"Parceiro {index}: {nome}")

            except Exception as e:
                logging.error(f"Erro ao processar o card {index}: {e}")
                continue

    except Exception as e:
        logging.error(f"Erro ao extrair parceiros: {e}")

    return parceiros

def simular_cliques_e_extrair_links(driver, parceiros):
    """
    Simula cliques nos elementos <a> dos cards para extrair as URLs e atualiza o banco de dados.
    """
    if not parceiros:
        logging.warning("Nenhum parceiro para processar cliques.")
        return

    table_empresas = get_env_var("TABLE_EMPRESAS_ESF")

    for parceiro in parceiros:
        try:
            nome = parceiro["nome"]
            card_index = parceiro["card_index"]

            logging.info(f"Processando clique para a empresa '{nome}' (Card {card_index})")

            # Localizar o card pelo índice
            # nth-of-type é 1-based
            card_css_selector = f"{SELECTOR_CARDS}:nth-of-type({card_index}) {SELECTOR_LINK}"
            try:
                botao_link = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, card_css_selector))
                )
            except Exception as e:
                logging.error(f"Não foi possível localizar o botão de link para '{nome}': {e}")
                continue

            # Scroll até o botão para garantir que está visível
            driver.execute_script("arguments[0].scrollIntoView(true);", botao_link)
            time.sleep(1)  # Pausa para garantir o scroll

            # Simular o clique
            try:
                botao_link.click()
                logging.info(f"Clicado no link da empresa '{nome}'.")
            except Exception as e:
                logging.error(f"Erro ao clicar no link da empresa '{nome}': {e}")
                # Tentar clicar via JavaScript como fallback
                try:
                    driver.execute_script("arguments[0].click();", botao_link)
                    logging.info(f"Clicado no link via JavaScript para a empresa '{nome}'.")
                except Exception as js_e:
                    logging.error(f"Falha ao clicar via JavaScript para a empresa '{nome}': {js_e}")
                    continue

            # Esperar a nova página carregar
            try:
                WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                url_atual = driver.current_url
                logging.info(f"URL obtida para '{nome}': {url_atual}")
            except Exception as e:
                logging.error(f"Timeout ao esperar a nova página carregar para '{nome}': {e}")
                continue

            # Atualizar o link no banco de dados
            try:
                with obter_conexao() as connection:
                    if connection:
                        with connection.cursor() as cursor:
                            cursor.execute(f"UPDATE {table_empresas} SET link = %s WHERE id = %s", (url_atual, parceiro["empresa_id"]))
                            connection.commit()
                            logging.info(f"Link atualizado para a empresa '{nome}': {url_atual}")
            except Exception as e:
                logging.error(f"Erro ao atualizar o link no banco para '{nome}': {e}")

            # Navegar de volta para a página principal
            driver.back()
            logging.info(f"Retornando para a página principal após processar '{nome}'.")

            # Aguardar os cards carregarem novamente
            try:
                WebDriverWait(driver, 20).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, SELECTOR_CARDS))
                )
                logging.info("Página principal recarregada com sucesso.")
            except:
                logging.error("Timeout ao esperar a página principal recarregar.")
                break

            # Pausa para evitar sobrecarga e garantir estabilidade
            time.sleep(2)

        except Exception as e:
            logging.error(f"Erro inesperado ao processar '{nome}': {e}")
            continue

def main():
    with obter_conexao() as connection:
        if not connection:
            logging.error("Não foi possível obter a conexão com o banco de dados. Encerrando o bot.")
            return

        # Configurar o Selenium
        driver = configurar_selenium()
        if not driver:
            logging.error("Não foi possível iniciar o WebDriver. Encerrando o bot.")
            return

        try:
            # Extrair parceiros usando BeautifulSoup
            parceiros = extrair_parceiros(driver, connection)
            if parceiros:
                # Simular cliques e extrair links
                simular_cliques_e_extrair_links(driver, parceiros)
            else:
                logging.warning("Nenhum parceiro encontrado para processar.")
        finally:
            driver.quit()
            logging.info("Navegador fechado.")

    logging.info("Bot finalizado com sucesso.")

if __name__ == "__main__":
    main()
