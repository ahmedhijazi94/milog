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
    StaleElementReferenceException,
    WebDriverException
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
    try:
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
    finally:
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

    try:
        driver = webdriver.Chrome(options=chrome_options)
        driver.set_window_size(1920, 1080)
        return driver
    except WebDriverException as e:
        print(f"[ERROR] Não foi possível iniciar o WebDriver do Selenium: {e}")
        return None


def obter_empresa_id(nome_empresa, connection, table_empresas):
    """
    Recupera o ID da empresa pelo nome. Assume que o bot atual já populou a tabela.
    """
    cursor = connection.cursor()
    try:
        cursor.execute(f"SELECT id FROM {table_empresas} WHERE nome = %s", (nome_empresa,))
        empresa = cursor.fetchone()
        if empresa:
            return empresa[0]
        else:
            print(f"[WARN] Empresa '{nome_empresa}' não encontrada na tabela.")
            return None
    finally:
        cursor.close()


def atualizar_link_no_banco(connection, table_empresas, empresa_id, link_novo):
    """
    Atualiza o campo 'link' para a empresa especificada se for diferente.
    """
    cursor = connection.cursor()
    try:
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
    finally:
        cursor.close()


def processar_cards(driver, connection, table_empresas):
    """
    Itera sobre cada card, clica no botão 'Ir para regras do parceiro', obtém a URL e atualiza o banco.
    """
    try:
        cards = driver.find_elements(By.CSS_SELECTOR, "div.parity__card")
        num_cards = len(cards)
        print(f"[INFO] Total de cards a serem processados: {num_cards}")

        for i in range(num_cards):
            try:
                # Re-encontrar os cards para evitar StaleElementReferenceException
                cards = WebDriverWait(driver, 10).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.parity__card"))
                )
                card = cards[i]

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
                    print("[WARN] Botão 'Ir para regras do parceiro' não encontrado.")
                    continue

                # Simular o clique no botão
                try:
                    botao_know_more.click()
                    print(f"[INFO] Clicado no botão 'Ir para regras do parceiro' para a empresa '{nome_empresa}'.")
                except (ElementClickInterceptedException, StaleElementReferenceException) as e:
                    print(f"[ERROR] Não foi possível clicar no botão para a empresa '{nome_empresa}': {e}")
                    continue

                # Esperar a nova página carregar
                try:
                    WebDriverWait(driver, 20).until(
                        EC.presence_of_element_located((By.TAG_NAME, "body"))
                    )
                    url_atual = driver.current_url
                    print(f"[INFO] URL obtida: {url_atual}")
                except TimeoutException:
                    print("[ERROR] Timeout ao esperar a nova página carregar.")
                    continue

                # Atualizar o banco de dados
                empresa_id = obter_empresa_id(nome_empresa, connection, table_empresas)
                if empresa_id:
                    atualizar_link_no_banco(connection, table_empresas, empresa_id, url_atual)

                # Navegar de volta para a página principal
                driver.back()
                print(f"[INFO] Retornando para a página principal.")

                # Esperar a página principal carregar novamente
                try:
                    WebDriverWait(driver, 20).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "div.parity__card"))
                    )
                    print("[INFO] Página principal recarregada com sucesso.")
                except TimeoutException:
                    print("[ERROR] Timeout ao esperar a página principal recarregar.")
                    break

                # Pausa para evitar sobrecarga e garantir que a página esteja estável
                time.sleep(2)

            except IndexError:
                print(f"[ERROR] Índice {i} fora do intervalo. Número de cards pode ter mudado.")
                break
            except Exception as e:
                print(f"[ERROR] Ocorreu um erro inesperado: {e}")
                continue

    except Exception as e:
        print(f"[ERROR] Erro durante o processamento dos cards: {e}")


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
    if not driver:
        connection.close()
        return

    url = "https://www.livelo.com.br/ganhe-pontos-compre-e-pontue"

    try:
        print("[INFO] Abrindo página principal...")
        driver.get(url)

        # Tenta clicar no botão de cookies
        try:
            WebDriverWait(driver, 10).until(
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
            print("[INFO] Cards encontrados na página principal.")
        except TimeoutException:
            print("[ERROR] Timeout ao esperar os cards na página principal.")
            driver.quit()
            connection.close()
            return

        # Processar os cards para obter e salvar os links
        processar_cards(driver, connection, table_empresas)

    finally:
        # Fechar o navegador e a conexão com o banco
        driver.quit()
        connection.close()
        print("[INFO] Bot finalizado com sucesso.")


if __name__ == "__main__":
    main()
