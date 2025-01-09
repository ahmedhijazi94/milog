import os
import mysql.connector
import re
import time
from datetime import datetime
from collections import Counter
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
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

def criar_tabelas(connection):
    """
    Cria as tabelas no banco de dados caso elas não existam.
    Lê os nomes das tabelas exclusivamente das variáveis de ambiente.
    """
    table_empresas = get_env_var("TABLE_EMPRESAS_LIV")
    table_pontuacao = get_env_var("TABLE_PONTUACAO_LIV")

    try:
        cursor = connection.cursor()

        # Criação da tabela para as empresas
        create_empresas_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_empresas} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            nome VARCHAR(255) UNIQUE NOT NULL,
            logo VARCHAR(255),
            label_pontuacao VARCHAR(50) DEFAULT 'Sem Dados',
            sobre TEXT DEFAULT NULL -- Novo campo para descrição sobre a empresa
        );
        """
        cursor.execute(create_empresas_table_query)

        # Criação da tabela de pontuação
        create_pontuacao_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_pontuacao} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            data_hora_coleta DATETIME NOT NULL,
            moeda VARCHAR(10),
            pontuacao FLOAT,
            pontuacao_clube_livelo FLOAT,
            empresa_id INT,
            descricao_text TEXT,
            regra TEXT DEFAULT NULL, -- Novo campo para regra
            regulamento_doc VARCHAR(255) DEFAULT NULL, -- Novo campo para URL do regulamento
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
    table_empresas = get_env_var("TABLE_EMPRESAS_LIV")

    cursor = connection.cursor()
    cursor.execute(f"SELECT id, logo, sobre FROM {table_empresas} WHERE nome = %s", (nome_empresa,))
    empresa = cursor.fetchone()

    if empresa:
        # Empresa já existe, vamos atualizar o logo se for diferente
        empresa_id, current_logo, current_sobre = empresa
        if current_logo != logo:
            cursor.execute(f"UPDATE {table_empresas} SET logo = %s WHERE id = %s", (logo, empresa_id))
            connection.commit()
            print(f"[INFO] Logo atualizado para a empresa '{nome_empresa}'.")
        return empresa_id
    else:
        # Inserir nova empresa
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

def extrair_detalhes_parceiro(driver):
    """
    Extraí detalhes adicionais de um parceiro a partir da página de detalhes.
    
    Args:
        driver: Instância do Selenium WebDriver já na página de detalhes.
    
    Returns:
        dict: Dicionário com 'regra', 'regulamento_doc' e 'sobre'.
    """
    detalhes = {
        "regra": None,
        "regulamento_doc": None,
        "sobre": None
    }

    try:
        # Extrair 'regra'
        regra_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.partners-important-guidelines-component_one-content-description"))
        )
        detalhes["regra"] = regra_element.text.strip()
        print("[INFO] 'regra' extraída com sucesso.")

        # Extrair 'regulamento_doc'
        regulamento_element = driver.find_element(By.CSS_SELECTOR, "a.partners-important-guidelines-component_two-rules")
        detalhes["regulamento_doc"] = regulamento_element.get_attribute("href").strip()
        print("[INFO] 'regulamento_doc' extraída com sucesso.")

        # Extrair 'sobre'
        try:
            sobre_element = driver.find_element(By.CSS_SELECTOR, "div.partners-faq-component_one-content-description")
            detalhes["sobre"] = sobre_element.text.strip()
            print("[INFO] 'sobre' extraído com sucesso.")
        except:
            print("[WARN] Elemento 'sobre' não encontrado.")
            detalhes["sobre"] = None

    except Exception as e:
        print(f"[ERROR] Erro ao extrair detalhes do parceiro: {e}")

    return detalhes

def extrair_parceiros(connection):
    """
    Acessa a página da Livelo, coleta as informações dos cards de parceiros
    e retorna uma lista de dicionários com:
      - nome
      - moeda
      - descricao_text
      - pontuacao
      - pontuacao_clube_livelo
      - regra
      - regulamento_doc
      - sobre
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

    try:
        print("[INFO] Abrindo página...")
        driver.get(url)

        # Tenta clicar no botão de cookies
        try:
            WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler"))
            ).click()
            print("[INFO] Cookies aceitos.")
        except:
            print("[INFO] Nenhum pop-up de cookies encontrado.")

        # Espera os cards carregarem
        try:
            WebDriverWait(driver, 20).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.parity__card"))
            )
            print("[INFO] Cards encontrados.")
        except:
            print("[ERROR] Timeout ao esperar os cards.")
            driver.quit()
            return []

        time.sleep(2)  # Pausa para garantir o carregamento
        html = driver.page_source
        soup = BeautifulSoup(html, "html.parser")
        div_cards = soup.find_all("div", class_="parity__card")
        if not div_cards:
            print("[ERROR] Não foi possível encontrar os cards.")
            driver.quit()
            return []

        print(f"[INFO] Total de cards encontrados: {len(div_cards)}")

        parceiros = []
        for index, card in enumerate(div_cards, start=1):
            print(f"[INFO] Processando parceiro {index}/{len(div_cards)}")
            img_tag = card.find("img", class_="parity__card--img")
            nome = img_tag.get("alt", "Nome não encontrado") if img_tag else "Nome não encontrado"
            
            # Logo
            logo_relativo = img_tag.get("src", "") if img_tag else ""
            # Completar a URL se for relativa
            if logo_relativo.startswith("/"):
                logo_completo = f"https://www.livelo.com.br{logo_relativo}"
            else:
                logo_completo = logo_relativo

            descricao_principal = ""
            info_value = card.find("div", class_="info__value")
            if info_value:
                descricao_principal = info_value.get_text(" ", strip=True)

            clube_livelo = card.find("div", class_="info__club")
            texto_clube_livelo = clube_livelo.get_text(" ", strip=True) if clube_livelo else ""

            # Ajusta descrição completa para parse_descricao
            if texto_clube_livelo:
                descricao_principal = descricao_principal.lstrip("ou até ").strip()
                descricao_completa = f"{texto_clube_livelo} no Clube Livelo ou até {descricao_principal}"
            else:
                descricao_completa = descricao_principal

            moeda, pontuacao, pontuacao_clube = parse_descricao(descricao_completa)

            empresa_id = obter_empresa_id(nome, logo_completo, connection)

            # Extrair a URL 'knowmore' via Knockout.js
            try:
                # Localizar o link 'Know More' dentro do card
                know_more_link = card.find("a", class_="button__knowmore--link")
                if know_more_link:
                    # Usar JavaScript para obter o contexto de dados do Knockout.js
                    detalhes_url = driver.execute_script("return ko.dataFor(arguments[0]).knowmore;", know_more_link)
                    if detalhes_url:
                        # Completar a URL se for relativa
                        if detalhes_url.startswith("/"):
                            detalhes_url = f"https://www.livelo.com.br{detalhes_url}"
                        
                        print(f"[INFO] Navegando para detalhes do parceiro '{nome}' em {detalhes_url}")
                        # Abrir a URL de detalhes em uma nova aba
                        driver.execute_script("window.open(arguments[0], '_blank');", detalhes_url)
                        driver.switch_to.window(driver.window_handles[1])  # Mudar para a nova aba

                        # Extrair os detalhes
                        detalhes = extrair_detalhes_parceiro(driver)

                        # Fechar a aba de detalhes e voltar para a principal
                        driver.close()
                        driver.switch_to.window(driver.window_handles[0])
                        print("[INFO] Retornou para a página principal.")
                        time.sleep(2)  # Pausa para garantir que a página principal esteja pronta
                    else:
                        print(f"[WARN] Href vazio para o parceiro '{nome}'.")
                        detalhes = {
                            "regra": None,
                            "regulamento_doc": None,
                            "sobre": None
                        }
                else:
                    print(f"[WARN] Link 'Know More' não encontrado para o parceiro '{nome}'.")
                    detalhes = {
                        "regra": None,
                        "regulamento_doc": None,
                        "sobre": None
                    }
            except Exception as e:
                print(f"[ERROR] Erro ao extrair 'knowmore' para o parceiro '{nome}': {e}")
                detalhes = {
                    "regra": None,
                    "regulamento_doc": None,
                    "sobre": None
                }

            parceiro_info = {
                "empresa_id": empresa_id,
                "moeda": moeda,
                "pontuacao": pontuacao,
                "pontuacao_clube_livelo": pontuacao_clube,
                "descricao_text": descricao_completa,
                "regra": detalhes["regra"],
                "regulamento_doc": detalhes["regulamento_doc"],
                "sobre": detalhes["sobre"]
            }

            parceiros.append(parceiro_info)

        driver.quit()
        return parceiros

def calcular_moda(pontuacoes):
    """
    Calcula a moda de uma lista de pontuações.

    Args:
        pontuacoes (list of float): Lista de pontuações.

    Returns:
        float: Moda da lista. Se houver múltiplas modas, retorna a maior.
    """
    if not pontuacoes:
        return 0

    contador = Counter(pontuacoes)
    max_freq = max(contador.values())
    modas = [pont for pont, freq in contador.items() if freq == max_freq]
    return max(modas)  # Retorna a maior moda se houver múltiplas

def calcular_label_pontuacao(pontuacoes):
    """
    Calcula a label de pontuação com base nas pontuações históricas.

    Args:
        pontuacoes (list of float): Lista de pontuações do parceiro.

    Returns:
        str: Label da pontuação.
    """
    if not pontuacoes:
        return "Sem Dados"

    # Calculando as métricas necessárias
    min_val = min(pontuacoes)
    max_val = max(pontuacoes)
    mode_val = calcular_moda(pontuacoes)  # Função para calcular a moda
    last_val = pontuacoes[-1]  # Última pontuação inserida

    # Definindo os thresholds
    excellent_threshold = mode_val * 2
    good_threshold = mode_val
    poor_threshold = mode_val / 2

    # Definindo as labels
    if max_val <= 0 or min_val == max_val:
        return "Pontuação Normal"

    if last_val > excellent_threshold:
        return "Ótima Pontuação"
    elif good_threshold < last_val <= excellent_threshold:
        return "Boa Pontuação"
    elif last_val == mode_val:
        return "Pontuação Normal"
    elif poor_threshold <= last_val < good_threshold:
        return "Pouco Abaixo do Normal"
    else:
        return "Má Pontuação"

def salvar_relatorio_mysql(parceiros, connection):
    """
    Insere os dados de pontuação no banco de dados MySQL.
    Relaciona com a empresa e inclui a descrição.
    Atualiza a label_pontuacao na tabela de empresas.
    Atualiza os novos campos: regra, regulamento_doc e sobre.

    Args:
        parceiros (list of dict): Lista de parceiros com suas pontuações.
        connection: Objeto de conexão MySQL.
    """
    if not parceiros:
        print("[WARN] Lista de parceiros vazia; não há o que salvar.")
        return

    # Lê apenas da variável de ambiente (sem fallback)
    table_pontuacao = get_env_var("TABLE_PONTUACAO_LIV")
    table_empresas = get_env_var("TABLE_EMPRESAS_LIV")

    try:
        cursor = connection.cursor()
        insert_query = f"""
            INSERT INTO {table_pontuacao} (
                data_hora_coleta, moeda, pontuacao, pontuacao_clube_livelo, empresa_id, descricao_text, regra, regulamento_doc
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """

        for parceiro in parceiros:
            data_hora_coleta = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            cursor.execute(insert_query, (
                data_hora_coleta,
                parceiro["moeda"],
                parceiro["pontuacao"],
                parceiro["pontuacao_clube_livelo"],
                parceiro["empresa_id"],
                parceiro["descricao_text"],
                parceiro["regra"],
                parceiro["regulamento_doc"]
            ))

        connection.commit()
        print("[INFO] Dados inseridos no banco de dados com sucesso.")

        # Após inserir, atualizar a label_pontuacao e 'sobre' para cada parceiro
        for parceiro in parceiros:
            empresa_id = parceiro["empresa_id"]

            # Recuperar todas as pontuações do parceiro
            cursor.execute(f"""
                SELECT pontuacao FROM {table_pontuacao}
                WHERE empresa_id = %s ORDER BY data_hora_coleta ASC
            """, (empresa_id,))
            resultados = cursor.fetchall()
            pontuacoes = [row[0] for row in resultados]

            # Calcular a label
            label = calcular_label_pontuacao(pontuacoes)

            # Atualizar a tabela de empresas
            cursor.execute(f"""
                UPDATE {table_empresas}
                SET label_pontuacao = %s
                WHERE id = %s
            """, (label, empresa_id))
            print(f"[INFO] label_pontuacao atualizado para a empresa ID {empresa_id}: {label}")

            # Atualizar o campo 'sobre' se estiver disponível e ainda não tiver sido atualizado
            sobre = parceiro.get("sobre")
            if sobre:
                cursor.execute(f"""
                    SELECT sobre FROM {table_empresas}
                    WHERE id = %s
                """, (empresa_id,))
                current_sobre = cursor.fetchone()[0]
                if not current_sobre:
                    cursor.execute(f"""
                        UPDATE {table_empresas}
                        SET sobre = %s
                        WHERE id = %s
                    """, (sobre, empresa_id))
                    print(f"[INFO] Campo 'sobre' atualizado para a empresa ID {empresa_id}.")
                else:
                    print(f"[INFO] Campo 'sobre' já está preenchido para a empresa ID {empresa_id}.")
            else:
                print(f"[INFO] Nenhuma informação 'sobre' para atualizar para a empresa ID {empresa_id}.")

        connection.commit()
        print("[INFO] Labels de pontuação e descrições atualizadas com sucesso.")
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
