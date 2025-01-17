import os
import mysql.connector
import time
from datetime import datetime, timedelta
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

def obter_primeira_link_update_date(connection, table_empresas):
    cursor = connection.cursor()
    try:
        cursor.execute(f"SELECT link_update_date FROM {table_empresas} ORDER BY id ASC LIMIT 1")
        resultado = cursor.fetchone()
        if resultado:
            return resultado[0]  # Pode ser None ou datetime
        else:
            print(f"[WARN] Não há registros na tabela '{table_empresas}'.")
            return None
    finally:
        cursor.close()

def deve_executar(link_update_date, meses=1):
    if not link_update_date:
        return True
    try:
        ultima_atualizacao = link_update_date
        if isinstance(ultima_atualizacao, str):
            ultima_atualizacao = datetime.strptime(ultima_atualizacao, "%Y-%m-%d %H:%M:%S")
    except Exception as e:
        print(f"[WARN] Formato inesperado para link_update_date: {e}")
        return True
    agora = datetime.now()
    delta = agora - ultima_atualizacao
    return delta >= timedelta(days=30 * meses)

def obter_empresa_id(nome_empresa, connection, table_empresas):
    cursor = connection.cursor()
    try:
        cursor.execute(f"SELECT id, link FROM {table_empresas} WHERE nome = %s", (nome_empresa,))
        empresa = cursor.fetchone()
        if empresa:
            return empresa[0], empresa[1]  # Retorna id e link atual
        else:
            print(f"[WARN] Empresa '{nome_empresa}' não encontrada na tabela.")
            return None, None
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
                current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                cursor.execute(f"UPDATE {table_empresas} SET link = %s, link_update_date = %s WHERE id = %s", 
                               (link_novo, current_time, empresa_id))
                connection.commit()
                print(f"[INFO] Link atualizado para a empresa ID {empresa_id}: {link_novo}")
            except mysql.connector.Error as err:
                print(f"[ERROR] Erro ao atualizar o link para a empresa ID {empresa_id}: {err}")
        else:
            print(f"[INFO] Link para a empresa ID {empresa_id} já está atualizado.")
    finally:
        cursor.close()

def extrair_link_sem_clicar(driver, botao_know_more):
    """
    Tenta extrair o link 'knowmore' diretamente dos atributos ou do 'data-bind'.
    Retorna o link se encontrado, caso contrário, retorna None.
    """
    try:
        # Extrai o valor do atributo 'href', se presente
        href = botao_know_more.get_attribute('href')
        if href and href.strip():
            return href.strip()
        
        # Caso o 'href' não esteja definido, tenta extrair do 'data-bind'
        data_bind = botao_know_more.get_attribute('data-bind')
        if data_bind:
            # Usa regex para extrair o valor de 'knowmore' dentro do 'data-bind'
            match = re.search(r"window\.location\.href\s*=\s*['\"]([^'\"]+)['\"]", data_bind)
            if match:
                return match.group(1).strip()
        
        return None
    except Exception as e:
        print(f"[WARN] Não foi possível extrair o link sem clicar: {e}")
        return None

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

def processar_cards(driver, connection, table_empresas):
    try:
        cards = driver.find_elements(By.CSS_SELECTOR, "div.parity__card")
        num_cards = len(cards)
        print(f"[INFO] Total de cards a serem processados: {num_cards}")

        for i in range(num_cards):
            try:
                # Re-encontrar os cards para evitar StaleElementReferenceException
                WebDriverWait(driver, 10).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.parity__card"))
                )
                cards = driver.find_elements(By.CSS_SELECTOR, "div.parity__card")
                if i >= len(cards):
                    print(f"[WARN] Número de cards mudou. Total atual: {len(cards)}. Pulando índice {i}.")
                    continue
                card = cards[i]

                # Extrair o nome da empresa
                try:
                    img_tag = card.find_element(By.CSS_SELECTOR, "img.parity__card--img")
                    nome_empresa = img_tag.get_attribute("alt")
                    print(f"[INFO] Processando empresa: {nome_empresa}")
                except NoSuchElementException:
                    print("[WARN] Nome da empresa não encontrado no card.")
                    continue

                # Obter o ID e link atual da empresa
                empresa_id, link_atual_db = obter_empresa_id(nome_empresa, connection, table_empresas)
                if empresa_id is None:
                    print(f"[WARN] Não foi possível obter ID para a empresa '{nome_empresa}'.")
                    continue

                # Encontrar o botão 'Ir para regras do parceiro'
                try:
                    botao_know_more = card.find_element(By.CSS_SELECTOR, "a.button__knowmore--link.gtm-link-event")
                except NoSuchElementException:
                    print("[WARN] Botão 'Ir para regras do parceiro' não encontrado.")
                    continue

                # Tentar extrair o link sem clicar
                link_extraido = extrair_link_sem_clicar(driver, botao_know_more)
                if link_extraido:
                    print(f"[INFO] Link extraído sem clicar: {link_extraido}")
                    # Verificar se o link está vazio ou diferente do banco
                    if not link_atual_db or link_atual_db != link_extraido:
                        print(f"[INFO] Link está vazio ou diferente. Atualizando no banco.")
                        atualizar_link_no_banco(connection, table_empresas, empresa_id, link_extraido)
                    else:
                        print(f"[INFO] Link para a empresa '{nome_empresa}' já está atualizado.")
                    continue  # Pula o clique, pois já extraiu o link
                else:
                    print(f"[INFO] Link não pôde ser extrato sem clicar. Processando o card.")

                # Fechar notificações que possam estar interferindo
                fechar_notificacoes(driver)

                # Scroll até o botão para garantir que está visível
                driver.execute_script("arguments[0].scrollIntoView(true);", botao_know_more)
                time.sleep(1)  # Pausa para garantir o scroll

                # Esperar que o botão esteja clicável
                try:
                    WebDriverWait(driver, 10).until(EC.element_to_be_clickable(botao_know_more))
                except TimeoutException:
                    print(f"[WARN] Botão 'Ir para regras do parceiro' não está clicável para a empresa '{nome_empresa}'.")
                    continue

                # Simular o clique no botão
                try:
                    botao_know_more.click()
                    print(f"[INFO] Clicado no botão 'Ir para regras do parceiro' para a empresa '{nome_empresa}'.")
                except (ElementClickInterceptedException, StaleElementReferenceException) as e:
                    print(f"[ERROR] Não foi possível clicar no botão para a empresa '{nome_empresa}': {e}")
                    # Tentar clicar via JavaScript como fallback
                    try:
                        driver.execute_script("arguments[0].click();", botao_know_more)
                        print(f"[INFO] Clicado no botão via JavaScript para a empresa '{nome_empresa}'.")
                    except Exception as js_e:
                        print(f"[ERROR] Falha ao clicar no botão via JavaScript para a empresa '{nome_empresa}': {js_e}")
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

    def main():
        # Conectar ao banco de dados
        connection = conectar_banco()
        if not connection:
            return

        # Obter o nome da tabela de empresas
        table_empresas = get_env_var("TABLE_EMPRESAS_LIV")

        # Verificar a data da última atualização
        link_update_date = obter_primeira_link_update_date(connection, table_empresas)
        if not deve_executar(link_update_date):
            print("[INFO] Links estão atualizados. Nenhuma ação necessária.")
            connection.close()
            return
        else:
            print("[INFO] Iniciando atualização dos links.")

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
