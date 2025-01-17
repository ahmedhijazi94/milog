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
    value = os.getenv(var_name)
    if not value:
        raise ValueError(f"A variável de ambiente '{var_name}' não está definida!")
    return value

def conectar_banco():
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

def garantir_campo_link(connection, table_empresas):
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
        return driver
    except WebDriverException as e:
        print(f"[ERROR] Não foi possível iniciar o WebDriver do Selenium: {e}")
        return None

def obter_empresa_id(nome_empresa, connection, table_empresas):
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

def fechar_notificacoes(driver):
    """
    Tenta fechar quaisquer notificações ou elementos que possam estar interceptando cliques.
    """
    try:
        # Exemplo: Fechar notificações com base em classes ou IDs conhecidos
        notificacoes = driver.find_elements(By.CSS_SELECTOR, "div.notifi__column.notifi__column--action")
        for notificacao in notificacoes:
            try:
                fechar_botao = notificacao.find_element(By.CSS_SELECTOR, "button.close")  # Atualize o seletor conforme necessário
                fechar_botao.click()
                print("[INFO] Notificação fechada.")
                time.sleep(1)  # Pausa para garantir que a notificação foi fechada
            except NoSuchElementException:
                continue
    except Exception as e:
        print(f"[WARN] Não foi possível fechar notificações: {e}")

def processar_cards_esf(driver, connection, table_empresas):
    try:
        cards = driver.find_elements(By.CSS_SELECTOR, "div.box-partner-custom")
        num_cards = len(cards)
        print(f"[INFO] Total de cards a serem processados: {num_cards}")

        for i in range(num_cards):
            try:
                # Re-encontrar os cards para evitar StaleElementReferenceException
                WebDriverWait(driver, 10).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.box-partner-custom"))
                )
                cards = driver.find_elements(By.CSS_SELECTOR, "div.box-partner-custom")
                card = cards[i]

                # Extrair o nome da empresa
                try:
                    img_tag = card.find_element(By.CSS_SELECTOR, "img")
                    nome_empresa = img_tag.get_attribute("alt")
                    print(f"[INFO] Processando empresa: {nome_empresa}")
                except NoSuchElementException:
                    print("[WARN] Nome da empresa não encontrado no card.")
                    continue

                # Extrair o link da empresa
                try:
                    link_element = card.find_element(By.TAG_NAME, "a")
                    link_relativo = link_element.get_attribute("href")
                    if not link_relativo.startswith("http"):
                        # Se o link for relativo, construí-lo com a base da URL
                        link_novo = f"https://www.esfera.com.vc{link_relativo}"
                    else:
                        link_novo = link_relativo
                    print(f"[INFO] Link encontrado para a empresa '{nome_empresa}': {link_novo}")
                except NoSuchElementException:
                    print("[WARN] Link não encontrado no card.")
                    continue

                # Fechar notificações que possam estar interferindo
                fechar_notificacoes(driver)

                # Atualizar o banco de dados
                empresa_id = obter_empresa_id(nome_empresa, connection, table_empresas)
                if empresa_id:
                    atualizar_link_no_banco(connection, table_empresas, empresa_id, link_novo)

                # Opcional: Se precisar interagir mais com a página, implementar aqui

                # Pausa para evitar sobrecarga e garantir que a página esteja estável
                time.sleep(1)

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

    # Obter o nome da tabela de empresas para Esfera
    table_empresas = get_env_var("TABLE_EMPRESAS_ESF")  # Certifique-se de definir esta variável de ambiente

    # Garantir que a tabela possui o campo 'link'
    garantir_campo_link(connection, table_empresas)

    # Configurar o Selenium
    driver = conectar_selenium()
    if not driver:
        connection.close()
        return

    # URL da Esfera
    url = "https://www.esfera.com.vc/c/ganhe-pontos/esf02163"

    try:
        print("[INFO] Abrindo página principal da Esfera...")
        driver.get(url)

        # Tenta clicar no botão de cookies, se existir
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
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.box-partner-custom"))
            )
            print("[INFO] Cards encontrados na página principal da Esfera.")
        except TimeoutException:
            print("[ERROR] Timeout ao esperar os cards na página principal da Esfera.")
            driver.quit()
            connection.close()
            return

        # Processar os cards para obter e salvar os links
        processar_cards_esf(driver, connection, table_empresas)

    finally:
        # Fechar o navegador e a conexão com o banco
        driver.quit()
        connection.close()
        print("[INFO] Bot finalizado com sucesso.")

if __name__ == "__main__":
    main()
