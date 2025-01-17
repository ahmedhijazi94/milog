import os
import mysql.connector
import time
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.common.exceptions import (
    NoSuchElementException,
    TimeoutException,
    ElementClickInterceptedException,
    StaleElementReferenceException
)
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


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
            print("[INFO] Conectado ao banco de dados.")
            return connection
    except mysql.connector.Error as err:
        print(f"[ERROR] Não foi possível conectar ao banco de dados: {err}")
        return None


def garantir_campo_link(connection, table_empresas):
    """
    Verifica se a tabela possui o campo 'link'. Se não, adiciona-o.
    """
    cursor = connection.cursor()
    cursor.execute("""
        SELECT COLUMN_NAME 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_NAME = 'link';
    """, (os.getenv("DB_NAME"), table_empresas))
    result = cursor.fetchone()
    if not result:
        try:
            cursor.execute(f"ALTER TABLE {table_empresas} ADD COLUMN link VARCHAR(2083);")
            connection.commit()
            print(f"[INFO] Coluna 'link' adicionada à tabela '{table_empresas}'.")
        except mysql.connector.Error as err:
            print(f"[ERROR] Não foi possível adicionar a coluna 'link': {err}")
    else:
        print(f"[INFO] A tabela '{table_empresas}' já possui a coluna 'link'.")
    cursor.close()


def conectar_selenium():
    """
    Configura e retorna uma instância do WebDriver do Selenium.
    """
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
    return driver


def extrair_cards(driver):
    """
    Extrai todos os elementos de cards de parceiros na página.
    """
    try:
        WebDriverWait(driver, 20).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.parity__card"))
        )
        print("[INFO] Cards encontrados.")
        cards = driver.find_elements(By.CSS_SELECTOR, "div.parity__card")
        print(f"[INFO] Total de cards encontrados: {len(cards)}")
        return cards
    except TimeoutException:
        print("[ERROR] Timeout ao esperar os cards.")
        return []


def obter_empresa_id(nome_empresa, connection, table_empresas):
    """
    Recupera o ID da empresa pelo nome. Assume que o bot atual já populou a tabela.
    """
    cursor = connection.cursor()
    cursor.execute(f"SELECT id FROM {table_empresas} WHERE nome = %s", (nome_empresa,))
    empresa = cursor.fetchone()
    cursor.close()
    if empresa:
        return empresa[0]
    else:
        print(f"[WARN] Empresa '{nome_empresa}' não encontrada na tabela.")
        return None


def atualizar_link_no_banco(connection, table_empresas, empresa_id, link_novo):
    """
    Atualiza o campo 'link' para a empresa especificada se for diferente.
    """
    cursor = connection.cursor()
    cursor.execute(f"SELECT link FROM {table_empresas} WHERE id = %s", (empresa_id,))
    resultado = cursor.fetchone()
    link_atual = resultado[0] if resultado else None

    if link_atual != link_novo:
        try:
            cursor.execute(f"UPDATE {table_empresas} SET link = %s WHERE id = %s", (link_novo, empresa_id))
            connection.commit()
            print(f"[INFO] Link atualizado para a empresa ID {empresa_id}: {link_novo}")
        except mysql.connector.Error as err:
            print(f"[ERROR] Erro ao atualizar o link para a empresa ID {empresa_id}: {err}")
    else:
        print(f"[INFO] Link para a empresa ID {empresa_id} já está atualizado.")


def processar_cards(driver, connection, table_empresas):
    """
    Itera sobre cada card, clica no botão 'Ir para regras do parceiro', obtém a URL e atualiza o banco.
    """
    cards = extrair_cards(driver)
    for index in range(len(cards)):
        try:
            # Re-encontrar os cards para evitar StaleElementReferenceException
            cards = driver.find_elements(By.CSS_SELECTOR, "div.parity__card")
            card = cards[index]
            # Extrair o nome da empresa
            try:
                img_tag = card.find_element(By.CSS_SELECTOR, "img.parity__card--img")
                nome_empresa = img_tag.get_attribute("alt")
                print(f"[INFO] Processando empresa: {nome_empresa}")
            except NoSuchElementException:
                print("[WARN] Nome da empresa não encontrado no card.")
                continue

            # Encontrar o botão 'Ir para regras do parceiro'
            try:
                botao_know_more = card.find_element(By.CSS_SELECTOR, "a.button__knowmore--link.gtm-link-event")
            except NoSuchElementException:
                print("[WARN] Botão 'Ir para regras do parceiro' não encontrado no card.")
                continue

            # Abrir link em nova aba para evitar perder a página principal
            link_know_more = botao_know_more.get_attribute("href")
            if not link_know_more:
                print("[WARN] Link do botão 'Ir para regras do parceiro' não encontrado.")
                continue

            # Abrir a URL em uma nova aba
            driver.execute_script("window.open(arguments[0], '_blank');", link_know_more)
            driver.switch_to.window(driver.window_handles[1])

            # Esperar a página carregar
            WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            url_atual = driver.current_url
            print(f"[INFO] URL obtida: {url_atual}")

            # Fechar a aba e voltar para a principal
            driver.close()
            driver.switch_to.window(driver.window_handles[0])

            # Atualizar o banco de dados
            empresa_id = obter_empresa_id(nome_empresa, connection, table_empresas)
            if empresa_id:
                atualizar_link_no_banco(connection, table_empresas, empresa_id, url_atual)

            # Pausa para evitar sobrecarga
            time.sleep(1)

        except StaleElementReferenceException:
            print("[ERROR] Referência do elemento está desatualizada. Reiniciando o processamento dos cards.")
            break
        except Exception as e:
            print(f"[ERROR] Ocorreu um erro inesperado: {e}")
            continue


def main():
    # Conectar ao banco de dados
    connection = conectar_banco()
    if not connection:
        return

    # Obter o nome da tabela de empresas
    table_empresas = get_env_var("TABLE_EMPRESAS_LIV")

    # Garantir que a tabela possui o campo 'link'
    garantir_campo_link(connection, table_empresas)

    # Configurar o Selenium
    driver = conectar_selenium()

    url = "https://www.livelo.com.br/ganhe-pontos-compre-e-pontue"

    print("[INFO] Abrindo página principal...")
    driver.get(url)

    # Tenta clicar no botão de cookies
    try:
        WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler"))
        ).click()
        print("[INFO] Cookies aceitos.")
    except TimeoutException:
        print("[INFO] Nenhum pop-up de cookies encontrado.")

    # Esperar os cards carregarem
    try:
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.parity__card"))
        )
    except TimeoutException:
        print("[ERROR] Timeout ao esperar os cards na página principal.")
        driver.quit()
        connection.close()
        return

    # Processar os cards
    processar_cards(driver, connection, table_empresas)

    # Fechar o navegador e a conexão com o banco
    driver.quit()
    connection.close()
    print("[INFO] Bot finalizado com sucesso.")


if __name__ == "__main__":
    main()
