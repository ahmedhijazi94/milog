import os
import mysql.connector
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    ElementClickInterceptedException,
    NoSuchElementException,
    TimeoutException,
)

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
            host=get_env_var("DB_HOST"),        # Host do banco de dados
            database=get_env_var("DB_NAME"),    # Nome do banco de dados
            user=get_env_var("DB_USER"),        # Usuário do banco de dados
            password=get_env_var("DB_PASSWORD") # Senha do banco de dados
        )
        if connection.is_connected():
            print("[INFO] Conectado ao banco de dados.")
            return connection
    except mysql.connector.Error as err:
        print(f"[ERROR] Não foi possível conectar ao banco de dados: {err}")
        return None

def criar_tabela_link(connection):
    """
    Adiciona a coluna 'link' na tabela de empresas, caso não exista.
    """
    table_empresas = get_env_var("TABLE_EMPRESAS_LIV")
    try:
        cursor = connection.cursor()
        # Verifica se a coluna 'link' já existe
        cursor.execute(f"SHOW COLUMNS FROM {table_empresas} LIKE 'link'")
        result = cursor.fetchone()
        if not result:
            # Adiciona a coluna 'link'
            alter_table_query = f"""
            ALTER TABLE {table_empresas}
            ADD COLUMN link VARCHAR(500) DEFAULT NULL
            """
            cursor.execute(alter_table_query)
            connection.commit()
            print(f"[INFO] Coluna 'link' adicionada à tabela '{table_empresas}'.")
        else:
            print(f"[INFO] Coluna 'link' já existe na tabela '{table_empresas}'.")
    except mysql.connector.Error as err:
        print(f"[ERROR] Não foi possível alterar a tabela: {err}")

def obter_empresa_por_nome(nome_empresa, connection):
    """
    Recupera a empresa pelo nome.
    """
    table_empresas = get_env_var("TABLE_EMPRESAS_LIV")
    cursor = connection.cursor()
    cursor.execute(f"SELECT id, link FROM {table_empresas} WHERE nome = %s", (nome_empresa,))
    return cursor.fetchone()

def atualizar_link_empresa(empresa_id, novo_link, connection):
    """
    Atualiza o campo 'link' da empresa se o link for diferente.
    """
    table_empresas = get_env_var("TABLE_EMPRESAS_LIV")
    cursor = connection.cursor()
    cursor.execute(f"SELECT link FROM {table_empresas} WHERE id = %s", (empresa_id,))
    resultado = cursor.fetchone()
    link_atual = resultado[0] if resultado else None

    if link_atual != novo_link:
        cursor.execute(f"UPDATE {table_empresas} SET link = %s WHERE id = %s", (novo_link, empresa_id))
        connection.commit()
        print(f"[INFO] Link atualizado para a empresa ID {empresa_id}: {novo_link}")
    else:
        print(f"[INFO] Link para a empresa ID {empresa_id} não mudou.")

def fechar_sobreposicoes(driver):
    """
    Tenta fechar quaisquer sobreposições que possam estar bloqueando cliques.
    """
    try:
        # Definir os seletores das sobreposições que precisam ser fechadas
        sobreposicoes = [
            # Se houver um botão de fechar para notificações
            {"by": By.CSS_SELECTOR, "value": "button.close-notification"},
            # Se houver um banner de cookies que ainda não foi fechado
            {"by": By.ID, "value": "onetrust-accept-btn-handler"},
            {"by": By.CSS_SELECTOR, "div.notifi__container"},
            # Adicione outros seletores conforme necessário
        ]
        
        for sobreposicao in sobreposicoes:
            try:
                elemento = WebDriverWait(driver, 2).until(
                    EC.element_to_be_clickable((sobreposicao["by"], sobreposicao["value"]))
                )
                elemento.click()
                print("[INFO] Sobreposição fechada.")
            except (NoSuchElementException, TimeoutException):
                pass  # Se o elemento não estiver presente, continue
    except Exception as e:
        print(f"[WARN] Erro ao tentar fechar sobreposições: {e}")

def extrair_links_regra(connection):
    """
    Percorre todos os cards de parceiros, extrai o link das regras e atualiza no banco de dados.
    """
    url = "https://www.livelo.com.br/ganhe-pontos-compre-e-pontue"

    chrome_options = Options()
    # Para depuração, execute o navegador com interface gráfica. Comente a linha abaixo.
    chrome_options.add_argument("--headless")  # Executa o Chrome em modo headless
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
    )

    driver = webdriver.Chrome(options=chrome_options)
    driver.set_window_size(1920, 1080)

    print("[INFO] Abrindo página para extrair links das regras...")
    driver.get(url)

    # Tenta clicar no botão de cookies
    try:
        WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler"))
        ).click()
        print("[INFO] Cookies aceitos.")
    except (NoSuchElementException, TimeoutException):
        print("[INFO] Nenhum pop-up de cookies encontrado.")

    # Espera os cards carregarem
    try:
        WebDriverWait(driver, 20).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.parity__card"))
        )
        print("[INFO] Cards encontrados.")
    except TimeoutException:
        print("[ERROR] Timeout ao esperar os cards.")
        driver.quit()
        return

    time.sleep(2)  # Pausa para garantir o carregamento

    # Coleta todos os nomes das empresas para iterar posteriormente
    cards = driver.find_elements(By.CSS_SELECTOR, "div.parity__card")
    total_cards = len(cards)
    print(f"[INFO] Total de cards encontrados: {total_cards}")

    # Coletar todos os nomes das empresas
    nomes_empresas = []
    for card in cards:
        try:
            img_tag = card.find_element(By.CSS_SELECTOR, "img.parity__card--img")
            nome = img_tag.get_attribute("alt") if img_tag else "Nome não encontrado"
            nomes_empresas.append(nome)
        except Exception as e:
            print(f"[ERROR] Erro ao coletar o nome de uma empresa: {e}")
            nomes_empresas.append("Nome não encontrado")

    # Iterar sobre a lista de nomes
    for index, nome in enumerate(nomes_empresas, start=1):
        try:
            print(f"[INFO] Processando {index}/{total_cards}: {nome}")

            # Recarregar os cards para evitar StaleElementReferenceException
            WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.parity__card"))
            )
            cards = driver.find_elements(By.CSS_SELECTOR, "div.parity__card")

            # Encontrar o card correspondente pelo nome
            card = None
            for c in cards:
                try:
                    img = c.find_element(By.CSS_SELECTOR, "img.parity__card--img")
                    img_nome = img.get_attribute("alt")
                    if img_nome == nome:
                        card = c
                        break
                except NoSuchElementException:
                    continue

            if not card:
                print(f"[WARN] Card para a empresa '{nome}' não encontrado.")
                continue

            # Fechar sobreposições antes de clicar
            fechar_sobreposicoes(driver)

            # Encontrar o botão "Ir para regras do parceiro" dentro do card
            try:
                botao_regras = card.find_element(By.CSS_SELECTOR, "a.button__knowmore--link")
            except NoSuchElementException:
                print(f"[WARN] Botão de regras não encontrado para a empresa '{nome}'.")
                continue

            # Tentar rolar até o botão para garantir que ele esteja visível
            driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", botao_regras)
            time.sleep(0.5)  # Aguarda a rolagem

            # Usar ActionChains para mover o mouse até o botão e clicar
            actions = ActionChains(driver)
            actions.move_to_element(botao_regras).click().perform()

            # Esperar a nova página carregar completamente
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )

            # Obter a URL atual da página de regras
            url_regra = driver.current_url
            print(f"[INFO] URL das regras para '{nome}': {url_regra}")

            # Atualizar o banco de dados
            empresa = obter_empresa_por_nome(nome, connection)
            if empresa:
                empresa_id, link_atual = empresa
                atualizar_link_empresa(empresa_id, url_regra, connection)
            else:
                print(f"[WARN] Empresa '{nome}' não encontrada no banco de dados.")

            # Navegar de volta para a página principal
            driver.back()

            # Esperar a página principal carregar novamente
            WebDriverWait(driver, 20).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.parity__card"))
            )

            # Pausar brevemente antes de processar o próximo card
            time.sleep(1)

        except ElementClickInterceptedException as e:
            print(f"[ERROR] Erro ao clicar no botão para a empresa '{nome}': {e}")
            # Captura de tela para depuração
            driver.save_screenshot(f"screenshot_error_{nome}.png")
            # Tentar fechar possíveis sobreposições novamente
            fechar_sobreposicoes(driver)
            # Tentar clicar via JavaScript como fallback
            try:
                botao_regras = card.find_element(By.CSS_SELECTOR, "a.button__knowmore--link")
                driver.execute_script("arguments[0].click();", botao_regras)

                # Esperar a nova página carregar completamente
                WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )

                # Obter a URL atual da página de regras
                url_regra = driver.current_url
                print(f"[INFO] URL das regras para '{nome}': {url_regra}")

                # Atualizar o banco de dados
                empresa = obter_empresa_por_nome(nome, connection)
                if empresa:
                    empresa_id, link_atual = empresa
                    atualizar_link_empresa(empresa_id, url_regra, connection)
                else:
                    print(f"[WARN] Empresa '{nome}' não encontrada no banco de dados.")

                # Navegar de volta para a página principal
                driver.back()

                # Esperar a página principal carregar novamente
                WebDriverWait(driver, 20).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.parity__card"))
                )

                # Pausar brevemente antes de processar o próximo card
                time.sleep(1)

            except Exception as e_inner:
                print(f"[ERROR] Fallback: Não foi possível clicar no botão para a empresa '{nome}': {e_inner}")
                # Captura de tela para depuração
                driver.save_screenshot(f"screenshot_error_fallback_{nome}.png")
                # Tentar navegar de volta caso algo falhe
                try:
                    driver.back()
                    WebDriverWait(driver, 20).until(
                        EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.parity__card"))
                    )
                    print("[INFO] Voltando para a página principal após erro no fallback.")
                except Exception as e_back:
                    print(f"[ERROR] Erro ao tentar voltar para a página principal: {e_back}")
                continue

        except Exception as e:
            print(f"[ERROR] Erro ao processar a empresa '{nome}': {e}")
            # Captura de tela para depuração
            driver.save_screenshot(f"screenshot_error_{nome}.png")
            # Tentar voltar para a página principal em caso de erro
            try:
                driver.get(url)
                WebDriverWait(driver, 20).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.parity__card"))
                )
                print("[INFO] Voltando para a página principal após erro.")
            except Exception as e_inner:
                print(f"[ERROR] Erro ao tentar voltar para a página principal: {e_inner}")
                # Captura de tela para depuração
                driver.save_screenshot(f"screenshot_error_back_{nome}.png")
                break  # Encerrar o loop se não for possível retornar

    driver.quit()
    print("[INFO] Extração e atualização de links concluída.")

def main():
    connection = conectar_banco()
    if connection:
        criar_tabela_link(connection)
        extrair_links_regra(connection)
        connection.close()

if __name__ == "__main__":
    main()
