import mysql.connector
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import time
import random
import csv
from datetime import datetime
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
import logging
import os
from urllib.parse import urlparse, parse_qs, urlencode
import uuid
from selenium.webdriver.support.ui import Select

# Set up logging
log_dir = "logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)
logging.basicConfig(
    filename=f"{log_dir}/scraper_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


# Conectar ao banco de dados MySQL e criar a tabela se não existir
def conectar_mysql():
    try:
        conexao = mysql.connector.connect(
            host="localhost",
            user="root",
            password="",
            database="advogados"
        )
        logging.info("Conexão com o banco de dados estabelecida!")
        print("Conexão com o banco de dados estabelecida!")

        cursor = conexao.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS advogados
            (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255),
                cedula VARCHAR(255),
                conselho VARCHAR(255),
                morada TEXT,
                estado VARCHAR(255),
                email VARCHAR(255),
                site VARCHAR(255),
                tipo VARCHAR(255),
                localidade VARCHAR(255),
                codigo_postal VARCHAR(255),
                telefone VARCHAR(255),
                data_inscricao VARCHAR(255),
                fax VARCHAR(255)
            )
        """)
        logging.info("Tabela 'advogados' verificada/recriada com sucesso!")
        print("Tabela 'advogados' verificada/recriada com sucesso!")

        # --- Criar a tabela sociedades se não existir ---
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sociedades
            (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255),
                conselho VARCHAR(255), 
                morada TEXT, 
                estado VARCHAR(255), 
                telefone VARCHAR(255), 
                email VARCHAR(255), 
                site VARCHAR(255), 
                tipo VARCHAR(255), 
                localidade VARCHAR(255), 
                registo VARCHAR(255),
                codigo_postal VARCHAR(255),
                data_constituicao VARCHAR(255),
                fax VARCHAR(255)
            )
        """)
        logging.info("Tabela 'sociedades' verificada/criada com sucesso!")
        print("Tabela 'sociedades' verificada/criada com sucesso!")

        # --- Criar a tabela estagiarios se não existir ---
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS estagiarios
            (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255),
                cedula VARCHAR(255),
                conselho VARCHAR(255),
                morada TEXT,
                estado VARCHAR(255),
                email VARCHAR(255),
                site VARCHAR(255),
                tipo VARCHAR(255),
                localidade VARCHAR(255),
                codigo_postal VARCHAR(255),
                data_inscricao VARCHAR(255),
                telefone VARCHAR(255),
                fax VARCHAR(255)
            )
        """)
        logging.info("Tabela 'estagiarios' verificada/criada com sucesso!")
        print("Tabela 'estagiarios' verificada/criada com sucesso!")

        # --- Criar a tabela agentes_execucao se não existir ---
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS agentes_execucao (
                id INT AUTO_INCREMENT PRIMARY KEY,
                nome VARCHAR(255),
                situacao VARCHAR(255),
                cedula VARCHAR(255),
                localidade VARCHAR(255),
                telefone VARCHAR(255),
                email VARCHAR(255),
                tipo VARCHAR(255)
            )
        """)
        logging.info("Tabela 'estagiarios' verificada/criada com sucesso!")
        print("Tabela 'estagiarios' verificada/criada com sucesso!")

        # --- Criar a tabela agentes_execucao se não existir ---
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS agentes_execucao (
                id INT AUTO_INCREMENT PRIMARY KEY,
                nome VARCHAR(255),
                situacao VARCHAR(255),
                cedula VARCHAR(255),
                localidade VARCHAR(255),
                telefone VARCHAR(255),
                email VARCHAR(255),
                tipo VARCHAR(255)
            )
        """)
        logging.info("Tabela 'agentes_execucao' verificada/criada com sucesso!")
        print("Tabela 'agentes_execucao' verificada/criada com sucesso!")

        conexao.commit()
        cursor.close()
        return conexao
    except mysql.connector.Error as e:
        logging.error(f"Erro ao conectar ao banco de dados: {e}")
        print(f"Erro ao conectar ao banco de dados: {e}")
        return None


# Função para inserir dados de advogados no banco de dados
def inserir_advogado(conexao, nome, cedula, conselho_regional, morada, estado_text, email='N/D',
                     site='N/D', tipo='N/D', localidade='N/D', codigo_postal='N/D', telefone='N/D',
                     data_inscricao='N/D', fax='N/D'):
    try:
        cursor = conexao.cursor()
        query = """
            INSERT INTO advogados (name, cedula, conselho, morada, estado, email, site, tipo, localidade, codigo_postal, telefone, data_inscricao, fax)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        dados = (
            str(nome or 'N/D'),
            str(cedula or 'N/D'),
            str(conselho_regional or 'N/D'),
            str(morada or 'N/D'),
            str(estado_text or 'N/D'),
            str(email or 'N/D'),
            str(site or 'N/D'),
            str(tipo or 'N/D'),
            str(localidade or 'N/D'),
            str(codigo_postal or 'N/D'),
            str(telefone or 'N/D'),
            str(data_inscricao or 'N/D'),
            str(fax or 'N/D'))
        logging.info(f"Executando query: {query % dados}")
        print(f"Executando query: {query % dados}")
        cursor.execute(query, dados)
        conexao.commit()
        cursor.close()
    except mysql.connector.Error as e:
        logging.error(f"Erro ao inserir advogado no banco de dados: {e}")
        print(f"Erro ao inserir advogado no banco de dados: {e}")
        raise


# Função para inserir dados de sociedades de advogados no banco de dados
def inserir_sociedades(conexao, nome, conselho_regional, morada, estado_text, telefone='N/D', email='N/D', site='N/D',
                       tipo='sociedade', localidade='N/D', registo='N/D', codigo_postal='N/D', data_constituicao='N/D',
                       fax='N/D'):
    try:
        cursor = conexao.cursor()
        query = """
            INSERT INTO sociedades (name, conselho, morada, estado, telefone, email, site, tipo, localidade, registo, codigo_postal, data_constituicao, fax)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        dados = (
            str(nome or 'N/D'),
            str(conselho_regional or 'N/D'),
            str(morada or 'N/D'),
            str(estado_text or 'N/D'),
            str(telefone or 'N/D'),
            str(email or 'N/D'),
            str(site or 'N/D'),
            str(tipo or 'sociedade'),
            str(localidade or 'N/D'),
            str(registo or 'N/D'),
            str(codigo_postal or 'N/D'),
            str(data_constituicao or 'N/D'),
            str(fax or 'N/D')
        )
        logging.info(f"Executando query: {query % dados}")
        print(f"Executando query: {query % dados}")
        cursor.execute(query, dados)
        conexao.commit()
        cursor.close()
    except mysql.connector.Error as e:
        logging.error(f"Erro ao inserir sociedade no banco de dados: {e}")
        print(f"Erro ao inserir sociedade no banco de dados: {e}")
        raise


# Função para inserir dados de estagiários no banco de dados
def inserir_estagiario(conexao, nome, cedula, conselho_regional, morada, estado_text, email='N/D',
                       site='N/D', tipo='N/D', localidade='N/D', codigo_postal='N/D', data_inscricao='N/D',
                       telefone='N/D', fax='N/D'):
    try:
        cursor = conexao.cursor()
        query = """
            INSERT INTO estagiarios 
            (name, cedula, conselho, morada, estado, email, site, tipo, localidade, codigo_postal, data_inscricao, telefone, fax)
            VALUES 
            (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        dados = (
            str(nome or 'N/D'),
            str(cedula or 'N/D'),
            str(conselho_regional or 'N/D'),
            str(morada or 'N/D'),
            str(estado_text or 'N/D'),
            str(email or 'N/D'),
            str(site or 'N/D'),
            str(tipo or 'estagiario'),
            str(localidade or 'N/D'),
            str(codigo_postal or 'N/D'),
            str(data_inscricao or 'N/D'),
            str(telefone or 'N/D'),
            str(fax or 'N/D')
        )
        logging.info(f"Executando query: {query % dados}")
        cursor.execute(query, dados)
        conexao.commit()
        logging.info(f"Estagiário {nome} inserido com sucesso!")
        print(f"✅ Estagiário {nome} inserido com sucesso!")
        cursor.close()
    except Exception as e:
        logging.error(f"Erro ao inserir estagiário {nome}: {e}")
        print(f"❌ Erro ao inserir estagiário {nome}: {e}")
        conexao.rollback()


# Função para inserir dados de agentes de execução no banco de dados
# (usada pelo scraping da OSAE)
def inserir_agente_execucao(conexao, nome, situacao, cedula, localidade, telefone, email, tipo='agente_execucao'):
    try:
        cursor = conexao.cursor()
        query = """
            INSERT INTO agentes_execucao (nome, situacao, cedula, localidade, telefone, email, tipo)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        dados = (
            str(nome or 'N/D'),
            str(situacao or 'N/D'),
            str(cedula or 'N/D'),
            str(localidade or 'N/D'),
            str(telefone or 'N/D'),
            str(email or 'N/D'),
            str(tipo or 'agente_execucao')
        )
        logging.info(f"Executando query: {query % dados}")
        cursor.execute(query, dados)
        conexao.commit()
        logging.info(f"Agente de Execução {nome} inserido com sucesso!")
        print(f"✅ Agente de Execução {nome} inserido com sucesso!")
        cursor.close()
    except Exception as e:
        logging.error(f"Erro ao inserir agente de execução {nome}: {e}")
        print(f"❌ Erro ao inserir agente de execução {nome}: {e}")
        conexao.rollback()


# Salvar em CSV
def save_to_csv(all_data, filename):
    if not all_data:
        logging.warning(f"Nenhum dado para salvar em CSV: {filename}")
        print(f"Nenhum dado para salvar em CSV: {filename}")
        return
    try:
        keys = set()
        for item in all_data:
            keys.update(item.keys())
        with open(filename, mode='w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=sorted(keys))
            writer.writeheader()
            writer.writerows(all_data)
        logging.info(f"Dados salvos em CSV: {filename}")
        print(f"📄 Dados salvos em CSV: {filename}")
    except Exception as e:
        logging.error(f"Erro ao salvar CSV {filename}: {e}")
        print(f"Erro ao salvar CSV {filename}: {e}")


# Configurações do Selenium
def configurar_driver():
    options = Options()
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_experimental_option('excludeSwitches', ['enable-automation'])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument(
        'user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')
    try:
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
        return driver
    except WebDriverException as e:
        logging.error(f"Erro ao configurar o WebDriver: {e}")
        print(f"Erro ao configurar o WebDriver: {e}")
        return None


# Função auxiliar para verificar CAPTCHA
def check_captcha(driver):
    for attempt in range(3):
        try:
            captcha = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'g-recaptcha')]"))
            )
            print("⚠️ CAPTCHA detectado! Por favor, resolva o CAPTCHA manualmente no navegador.")
            print("Aguardando 45 segundos para você resolver o CAPTCHA...")
            time.sleep(15)
            try:
                driver.find_element(By.XPATH, "//div[contains(@class, 'g-recaptcha')]")
                print("⚠️ CAPTCHA ainda presente. Tente novamente ou verifique o navegador.")
                return True
            except NoSuchElementException:
                print("CAPTCHA resolvido com sucesso!")
                return False
        except Exception:
            print("Nenhum CAPTCHA detectado.")
            return False
    print("⚠️ Falha ao lidar com o CAPTCHA após 3 tentativas.")
    return True


# Função auxiliar para extrair dados de um item
def extract_item_data(item, base_url, tipo):
    item_data = {}
    estado = item.find('div', class_='search-results__article-person-status')
    estado_text = estado.text.strip() if estado else "Sem estado"
    nome = item.find('h4', class_='search-results__article-person-title').text.strip() if item.find(
        'h4', class_='search-results__article-person-title') else 'N/D'

    item_data['Nome'] = nome
    item_data['Estado'] = estado_text
    item_data['Tipo'] = tipo

    details = item.find_all('ul', class_=['search-results__article-person-details-list', 'details-list'])
    for detail in details:
        items = detail.find_all('li', class_='search-results__details-list-item')
        for sub_item in items:
            label = sub_item.find('span', class_='search-results__details-list-item-label')
            value = sub_item.find('span', class_='search-results__details-list-item-description')
            if label and value:
                label_text = label.text.strip().lower()  # Convert label to lowercase for comparison
                value_text = value.text.strip()
                # Add print statement to see extracted data
                if tipo == 'estagiario':
                    print(f"  DEBUG: Extracted Label: '{label_text}', Value: '{value_text}'")
                # Use as chaves consistentes para armazenar os dados
                if "cédula" in label_text:
                    item_data['Cédula'] = value_text
                elif "conselho regional" in label_text:
                    item_data['Conselho Regional'] = value_text
                elif "morada" in label_text:
                    item_data['Morada'] = value_text
                elif "localidade" in label_text:
                    item_data['Localidade'] = value_text
                # Lógica aprimorada para Telefone, Data de Inscrição e Fax, especialmente para Estagiários
                if tipo == 'estagiario':
                    if "data de inscrição" in label_text or "data de inscricao" in label_text:
                        item_data['Data de Inscrição'] = value_text
                    elif "fax" in label_text or "fax registado" in label_text:
                        item_data['Fax'] = value_text
                    elif "telefone" in label_text:
                        item_data['Telefone'] = value_text
                    elif "email" in label_text:
                        item_data['Email'] = value_text
                    elif "código postal" in label_text or "codigo postal" in label_text:
                        item_data['Código Postal'] = value_text
                else:
                    # Existing logic for other types (advogados, sociedades, etc.)
                    if "telefone" in label_text:
                        item_data['Telefone'] = value_text
                    elif "email" in label_text:
                        item_data['Email'] = value_text
                    elif "registo" in label_text:
                        item_data['Registo'] = value_text
                    elif "código postal" in label_text or "codigo postal" in label_text:
                        item_data['Código Postal'] = value_text
                    elif "data de constituição" in label_text or "data de constituicao" in label_text:
                        item_data['Data de Constituição'] = value_text
                    elif "fax" in label_text or "fax registado" in label_text:
                        item_data['Fax'] = value_text
                    elif "data de inscrição" in label_text or "data de inscricao" in label_text:
                        item_data['Data de Inscrição'] = value_text

    # Garantir que todas as chaves esperadas existem, mesmo que com valor N/D
    # Inclui todas as chaves possíveis que podem ser extraídas/usadas
    expected_keys = ['Nome', 'Estado', 'Tipo', 'Cédula', 'Conselho Regional', 'Morada', 'Localidade',
                     'Telefone', 'Email', 'Registo', 'Código Postal', 'Data de Constituição',
                     'Fax', 'Data de Inscrição', 'Site']

    for key in expected_keys:
        if key not in item_data:
            item_data[key] = 'N/D'

    # Definir o campo Site (se não foi extraído)
    if 'Site' not in item_data or item_data['Site'] == 'N/D':
        # Se o Site não foi extraído, pode ser apropriado usar a base_url ou manter N/D
        # Vou manter N/D por agora, a menos que especificado o contrário.
        item_data['Site'] = 'N/D'

    return item_data


# Função para processar todas as páginas de uma só vez
def process_all_pages_at_once(driver, base_url, css_selector, max_pages=100, insert_func=None, conn=None,
                              single_page=False):
    print(f"Processando dados de {base_url}...")
    all_data = []
    # Determine the type based on the insert function provided
    if insert_func == inserir_estagiario:
        tipo = 'estagiario'
    elif insert_func == inserir_sociedades:
        tipo = 'sociedade'
    else:
        # Fallback to URL-based determination for other types if needed
        tipo = 'sociedade' if 'sociedades' in base_url.lower() else 'advogado'

    try:
        if single_page:
            print(f"Processando página única: {base_url}")
            driver.get(base_url)
            time.sleep(random.uniform(3, 5))

            if check_captcha(driver):
                driver.refresh()
                time.sleep(random.uniform(2, 4))

            for attempt in range(3):
                try:
                    WebDriverWait(driver, 30).until(
                        EC.visibility_of_any_elements_located((By.CSS_SELECTOR, css_selector))
                    )
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(random.uniform(2, 4))
                    break
                except Exception as e:
                    print(f"Tentativa {attempt + 1} falhou ao carregar página. Recarregando...")
                    logging.error(f"Tentativa {attempt + 1} falhou: {e}")
                    driver.refresh()
                    time.sleep(random.uniform(2, 4))
            else:
                print(f"⚠️ Não foi possível carregar a página: {base_url}")
                logging.error(f"Não foi possível carregar a página: {base_url}")
                with open(f"error_page_{base_url.split('/')[-2]}.html", 'w', encoding='utf-8') as f:
                    f.write(driver.page_source)
                return all_data

            html = driver.page_source
            soup = BeautifulSoup(html, 'lxml')
            items = soup.select(css_selector)

            if not items:
                print("Nenhum item encontrado nesta página.")
                logging.warning(f"Nenhum item encontrado em {base_url}")
                return all_data

            print(f"Encontrados {len(items)} itens na página.")
            for item in items:
                try:
                    item_data = extract_item_data(item, base_url, tipo)
                    if conn and conn.is_connected() and insert_func:
                        if tipo == 'sociedade':
                            insert_func(conn,
                                        item_data.get('Nome', 'N/D'),
                                        item_data.get('Conselho Regional', 'N/D'),
                                        item_data.get('Morada', 'N/D'),
                                        item_data.get('Estado', 'N/D'),
                                        item_data.get('Telefone', 'N/D'),
                                        item_data.get('Email', 'N/D'),
                                        item_data.get('Site', 'N/D'),
                                        item_data.get('Tipo', tipo),
                                        item_data.get('Localidade', 'N/D'),
                                        item_data.get('Registo', 'N/D'),
                                        item_data.get('Código Postal', 'N/D'),
                                        item_data.get('Data de Constituição', 'N/D'),
                                        item_data.get('Fax', 'N/D'))
                        elif tipo == 'estagiario':
                            insert_func(conn,
                                        item_data.get('Nome', 'N/D'),
                                        item_data.get('Cédula', 'N/D'),
                                        item_data.get('Conselho Regional', 'N/D'),
                                        item_data.get('Morada', 'N/D'),
                                        item_data.get('Estado', 'N/D'),
                                        item_data.get('Email', 'N/D'),
                                        item_data.get('Site', 'N/D'),
                                        item_data.get('Tipo', tipo),
                                        item_data.get('Localidade', 'N/D'),
                                        item_data.get('Código Postal', 'N/D'),
                                        item_data.get('Data de Inscrição', 'N/D'),
                                        item_data.get('Telefone', 'N/D'),
                                        item_data.get('Fax', 'N/D'))
                        else:
                            insert_func(conn,
                                        item_data.get('Nome', 'N/D'),
                                        item_data.get('Cédula', 'N/D'),
                                        item_data.get('Conselho Regional', 'N/D'),
                                        item_data.get('Morada', 'N/D'),
                                        item_data.get('Estado', 'N/D'),
                                        item_data.get('Email', 'N/D'),
                                        item_data.get('Site', 'N/D'),
                                        item_data.get('Tipo', tipo),
                                        item_data.get('Localidade', 'N/D'),
                                        item_data.get('Código Postal', 'N/D'),
                                        item_data.get('Telefone', 'N/D'),
                                        item_data.get('Data de Inscrição', 'N/D'))
                    all_data.append(item_data)
                    print(f"✅ Item {item_data['Nome']} processado com sucesso!")
                except Exception as e:
                    print(f"Erro ao processar item: {e}")
                    logging.error(f"Erro ao processar item: {e}")
                    continue

        else:
            current_page = 1
            max_attempts_without_data = 3
            attempts_without_data = 0

            while current_page <= max_pages:
                try:
                    try:
                        pagination_info = driver.find_element(By.CSS_SELECTOR,
                                                              '.pagination, .paging, [role="navigation"]')
                        total_pages_elements = pagination_info.find_elements(By.CSS_SELECTOR, 'a, span')
                        total_pages = 1
                        for elem in total_pages_elements:
                            text = elem.text.strip()
                            if text.isdigit():
                                total_pages = max(total_pages, int(text))
                        max_pages = min(max_pages, total_pages)
                        print(f"Total de páginas detectado: {total_pages}")
                    except NoSuchElementException:
                        print("Não foi possível determinar o total de páginas. Usando max_pages fornecido.")
                        logging.warning("Não foi possível determinar o total de páginas.")

                    current_url = driver.current_url
                    parsed_url = urlparse(current_url)
                    query_params = parse_qs(parsed_url.query)
                    query_params['page'] = [str(current_page)]
                    new_query = urlencode(query_params, doseq=True)
                    new_url = f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}?{new_query}"

                    print(f"Extraindo dados da página {current_page} ({new_url})...")
                    driver.get(new_url)
                    time.sleep(random.uniform(3, 5))

                    if check_captcha(driver):
                        driver.refresh()
                        time.sleep(random.uniform(2, 4))

                    for attempt in range(3):
                        try:
                            WebDriverWait(driver, 30).until(
                                EC.visibility_of_any_elements_located((By.CSS_SELECTOR, css_selector))
                            )
                            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                            time.sleep(random.uniform(2, 4))
                            break
                        except Exception as e:
                            print(f"Tentativa {attempt + 1} falhou ao carregar página {current_page}. Recarregando...")
                            logging.error(f"Tentativa {attempt + 1} falhou: {e}")
                            driver.refresh()
                            time.sleep(random.uniform(2, 4))
                    else:
                        print(f"⚠️ Não foi possível carregar a página {current_page}")
                        logging.error(f"Não foi possível carregar a página {current_page}")
                        with open(f"error_page_{base_url.split('/')[-2]}_{current_page}.html", 'w',
                                  encoding='utf-8') as f:
                            f.write(driver.page_source)
                        attempts_without_data += 1
                        if attempts_without_data >= max_attempts_without_data:
                            print(
                                f"⚠️ {max_attempts_without_data} páginas consecutivas sem dados. Encerrando paginação.")
                            break
                        current_page += 1
                        continue

                    html = driver.page_source
                    soup = BeautifulSoup(html, 'lxml')
                    items = soup.select(css_selector)

                    if not items:
                        print(f"Nenhum item encontrado na página {current_page}.")
                        logging.warning(f"Nenhum item encontrado na página {current_page} de {base_url}")
                        attempts_without_data += 1
                        if attempts_without_data >= max_attempts_without_data:
                            print(
                                f"⚠️ {max_attempts_without_data} páginas consecutivas sem dados. Encerrando paginação.")
                            break
                        current_page += 1
                        continue
                    else:
                        attempts_without_data = 0

                    print(f"Encontrados {len(items)} itens na página {current_page}.")
                    for item in items:
                        try:
                            item_data = extract_item_data(item, base_url, tipo)
                            if conn and conn.is_connected() and insert_func:
                                if tipo == 'sociedade':
                                    insert_func(conn,
                                                item_data.get('Nome', 'N/D'),
                                                item_data.get('Conselho Regional', 'N/D'),
                                                item_data.get('Morada', 'N/D'),
                                                item_data.get('Estado', 'N/D'),
                                                item_data.get('Telefone', 'N/D'),
                                                item_data.get('Email', 'N/D'),
                                                item_data.get('Site', 'N/D'),
                                                item_data.get('Tipo', tipo),
                                                item_data.get('Localidade', 'N/D'),
                                                item_data.get('Registo', 'N/D'),
                                                item_data.get('Código Postal', 'N/D'),
                                                item_data.get('Data de Constituição', 'N/D'),
                                                item_data.get('Fax', 'N/D'))
                                elif tipo == 'estagiario':
                                    insert_func(conn,
                                                item_data.get('Nome', 'N/D'),
                                                item_data.get('Cédula', 'N/D'),
                                                item_data.get('Conselho Regional', 'N/D'),
                                                item_data.get('Morada', 'N/D'),
                                                item_data.get('Estado', 'N/D'),
                                                item_data.get('Email', 'N/D'),
                                                item_data.get('Site', 'N/D'),
                                                item_data.get('Tipo', tipo),
                                                item_data.get('Localidade', 'N/D'),
                                                item_data.get('Código Postal', 'N/D'),
                                                item_data.get('Data de Inscrição', 'N/D'),
                                                item_data.get('Telefone', 'N/D'),
                                                item_data.get('Fax', 'N/D'))
                                else:
                                    insert_func(conn,
                                                item_data.get('Nome', 'N/D'),
                                                item_data.get('Cédula', 'N/D'),
                                                item_data.get('Conselho Regional', 'N/D'),
                                                item_data.get('Morada', 'N/D'),
                                                item_data.get('Estado', 'N/D'),
                                                item_data.get('Email', 'N/D'),
                                                item_data.get('Site', 'N/D'),
                                                item_data.get('Tipo', tipo),
                                                item_data.get('Localidade', 'N/D'),
                                                item_data.get('Código Postal', 'N/D'),
                                                item_data.get('Telefone', 'N/D'),
                                                item_data.get('Data de Inscrição', 'N/D'))
                            all_data.append(item_data)
                            print(f"✅ Item {item_data['Nome']} processado com sucesso!")
                        except Exception as e:
                            print(f"Erro ao processar item: {e}")
                            logging.error(f"Erro ao processar item: {e}")
                            continue

                    try:
                        next_button = driver.find_element(
                            By.XPATH,
                            '//a[contains(@class, "ws-pagination__nav") and .//span[contains(@class, "icon-chevron-right")]]'
                        )
                        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", next_button)
                        driver.execute_script("arguments[0].click();", next_button)
                        time.sleep(random.uniform(3, 5))
                    except NoSuchElementException:
                        if tipo == 'advogado' and current_page >= 6:
                            print(
                                "Limite de fallback por URL atingido (página 6) para advogados. Encerrando paginação.")
                            break
                        print("Botão 'Próximo' não encontrado. Encerrando paginação.")
                        break

                    current_page += 1
                    time.sleep(random.uniform(5, 10))

                except WebDriverException as e:
                    print(f"Erro ao processar página {current_page}: {e}")
                    logging.error(f"Erro ao processar página {current_page}: {e}")
                    attempts_without_data += 1
                    if attempts_without_data >= max_attempts_without_data:
                        print(f"⚠️ {max_attempts_without_data} páginas consecutivas sem dados. Encerrando paginação.")
                        break
                    current_page += 1
                    continue

    except Exception as e:
        print(f"Erro geral ao processar {base_url}: {e}")
        logging.error(f"Erro geral ao processar {base_url}: {e}")
        with open(f"error_page_{base_url.split('/')[-2]}.html", 'w', encoding='utf-8') as f:
            f.write(driver.page_source)

    if all_data:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"dados_{base_url.split('/')[-2]}_{timestamp}.csv"
        save_to_csv(all_data, filename)

    return all_data


# Função para coletar dados do portal de advogados
def coletar_advogados_lisboa(driver, conn):
    logging.info("Iniciando scraping de advogados...")
    print("🔎 Iniciando scraping de advogados...")
    base_url = 'https://portal.oa.pt/advogados/pesquisa-de-advogados/?l=&cg=L&ce=&n=&lo=&m=&cp=&op=&o=0&page=1'
    driver.get(base_url)
    try:
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'form'))
        )
        if check_captcha(driver):
            driver.refresh()
            time.sleep(random.uniform(2, 4))

        try:
            apenas_ativos = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH,
                                            "//label[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'apenas ativos')]/preceding-sibling::input[@type='checkbox'] | //input[@type='checkbox' and contains(@id, 'ativos')]"))
            )
            if not apenas_ativos.is_selected():
                driver.execute_script("arguments[0].click();", apenas_ativos)
                logging.info("Opção 'Apenas Ativos' marcada com sucesso!")
                print("Opção 'Apenas Ativos' marcada com sucesso!")
        except Exception as e:
            logging.warning(f"Erro ao marcar 'Apenas Ativos' (continuando sem marcar): {e}")
            print(f"Erro ao marcar 'Apenas Ativos' (continuando sem marcar): {e}")

        try:
            search_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR,
                                            'button[type="submit"], input[type="submit"], button.search-button, button.btn-primary'))
            )
            driver.execute_script("arguments[0].click();", search_button)
            time.sleep(random.uniform(3, 5))
        except Exception as e:
            logging.warning(f"Erro ao clicar no botão de pesquisa: {e}")
            print(f"Erro ao clicar no botão de pesquisa: {e}")

    except TimeoutException as e:
        logging.warning(f"Formulário de pesquisa não encontrado, prosseguindo com a coleta de dados: {e}")
        print(f"Formulário de pesquisa não encontrado, prosseguindo com a coleta de dados: {e}")

    return process_all_pages_at_once(driver, base_url,
                                     'article.search-results__article-person, .search-results article, .result-item',
                                     max_pages=100,
                                     insert_func=inserir_advogado, conn=conn)


def coletar_advogados_porto(driver, conn):
    logging.info("Iniciando scraping de advogados...")
    print("🔎 Iniciando scraping de advogados...")
    base_url = 'https://portal.oa.pt/advogados/pesquisa-de-advogados/?l=&cg=P&ce=&n=&lo=&m=&cp=&a=on&op=&o=0&g-recaptcha-response='
    driver.get(base_url)
    try:
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'form'))
        )
        if check_captcha(driver):
            driver.refresh()
            time.sleep(random.uniform(2, 4))

        try:
            apenas_ativos = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH,
                                            "//label[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'apenas ativos')]/preceding-sibling::input[@type='checkbox'] | //input[@type='checkbox' and contains(@id, 'ativos')]"))
            )
            if not apenas_ativos.is_selected():
                driver.execute_script("arguments[0].click();", apenas_ativos)
                logging.info("Opção 'Apenas Ativos' marcada com sucesso!")
                print("Opção 'Apenas Ativos' marcada com sucesso!")
        except Exception as e:
            logging.warning(f"Erro ao marcar 'Apenas Ativos' (continuando sem marcar): {e}")
            print(f"Erro ao marcar 'Apenas Ativos' (continuando sem marcar): {e}")

        try:
            search_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR,
                                            'button[type="submit"], input[type="submit"], button.search-button, button.btn-primary'))
            )
            driver.execute_script("arguments[0].click();", search_button)
            time.sleep(random.uniform(3, 5))
        except Exception as e:
            logging.warning(f"Erro ao clicar no botão de pesquisa: {e}")
            print(f"Erro ao clicar no botão de pesquisa: {e}")

    except TimeoutException as e:
        logging.warning(f"Formulário de pesquisa não encontrado, prosseguindo com a coleta de dados: {e}")
        print(f"Formulário de pesquisa não encontrado, prosseguindo com a coleta de dados: {e}")

    return process_all_pages_at_once(driver, base_url,
                                     'article.search-results__article-person, .search-results article, .result-item',
                                     max_pages=100,
                                     insert_func=inserir_advogado, conn=conn)


def coletar_advogados_coimbra(driver, conn):
    logging.info("Iniciando scraping de advogados...")
    print("🔎 Iniciando scraping de advogados...")
    base_url = 'https://portal.oa.pt/advogados/pesquisa-de-advogados/?l=&cg=C&ce=&n=&lo=&m=&cp=&a=on&op=&o=0&g-recaptcha-response='
    driver.get(base_url)
    try:
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'form'))
        )
        if check_captcha(driver):
            driver.refresh()
            time.sleep(random.uniform(2, 4))

        try:
            apenas_ativos = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH,
                                            "//label[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'apenas ativos')]/preceding-sibling::input[@type='checkbox'] | //input[@type='checkbox' and contains(@id, 'ativos')]"))
            )
            if not apenas_ativos.is_selected():
                driver.execute_script("arguments[0].click();", apenas_ativos)
                logging.info("Opção 'Apenas Ativos' marcada com sucesso!")
                print("Opção 'Apenas Ativos' marcada com sucesso!")
        except Exception as e:
            logging.warning(f"Erro ao marcar 'Apenas Ativos' (continuando sem marcar): {e}")
            print(f"Erro ao marcar 'Apenas Ativos' (continuando sem marcar): {e}")

        try:
            search_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR,
                                            'button[type="submit"], input[type="submit"], button.search-button, button.btn-primary'))
            )
            driver.execute_script("arguments[0].click();", search_button)
            time.sleep(random.uniform(3, 5))
        except Exception as e:
            logging.warning(f"Erro ao clicar no botão de pesquisa: {e}")
            print(f"Erro ao clicar no botão de pesquisa: {e}")

    except TimeoutException as e:
        logging.warning(f"Formulário de pesquisa não encontrado, prosseguindo com a coleta de dados: {e}")
        print(f"Formulário de pesquisa não encontrado, prosseguindo com a coleta de dados: {e}")

    return process_all_pages_at_once(driver, base_url,
                                     'article.search-results__article-person, .search-results article, .result-item',
                                     max_pages=100,
                                     insert_func=inserir_advogado, conn=conn)


def coletar_advogados_evora(driver, conn):
    logging.info("Iniciando scraping de advogados...")
    print("🔎 Iniciando scraping de advogados...")
    base_url = 'https://portal.oa.pt/advogados/pesquisa-de-advogados/?l=&cg=E&ce=&n=&lo=&m=&cp=&a=on&op=&o=0&g-recaptcha-response='
    driver.get(base_url)
    try:
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'form'))
        )
        if check_captcha(driver):
            driver.refresh()
            time.sleep(random.uniform(2, 4))

        try:
            apenas_ativos = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH,
                                            "//label[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'apenas ativos')]/preceding-sibling::input[@type='checkbox'] | //input[@type='checkbox' and contains(@id, 'ativos')]"))
            )
            if not apenas_ativos.is_selected():
                driver.execute_script("arguments[0].click();", apenas_ativos)
                logging.info("Opção 'Apenas Ativos' marcada com sucesso!")
                print("Opção 'Apenas Ativos' marcada com sucesso!")
        except Exception as e:
            logging.warning(f"Erro ao marcar 'Apenas Ativos' (continuando sem marcar): {e}")
            print(f"Erro ao marcar 'Apenas Ativos' (continuando sem marcar): {e}")

        try:
            search_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR,
                                            'button[type="submit"], input[type="submit"], button.search-button, button.btn-primary'))
            )
            driver.execute_script("arguments[0].click();", search_button)
            time.sleep(random.uniform(3, 5))
        except Exception as e:
            logging.warning(f"Erro ao clicar no botão de pesquisa: {e}")
            print(f"Erro ao clicar no botão de pesquisa: {e}")

    except TimeoutException as e:
        logging.warning(f"Formulário de pesquisa não encontrado, prosseguindo com a coleta de dados: {e}")
        print(f"Formulário de pesquisa não encontrado, prosseguindo com a coleta de dados: {e}")

    return process_all_pages_at_once(driver, base_url,
                                     'article.search-results__article-person, .search-results article, .result-item',
                                     max_pages=100,
                                     insert_func=inserir_advogado, conn=conn)


def coletar_advogados_faro(driver, conn):
    logging.info("Iniciando scraping de advogados...")
    print("🔎 Iniciando scraping de advogados...")
    base_url = 'https://portal.oa.pt/advogados/pesquisa-de-advogados/?l=&cg=F&ce=&n=&lo=&m=&cp=&a=on&op=&o=0&g-recaptcha-response='
    driver.get(base_url)
    try:
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'form'))
        )
        if check_captcha(driver):
            driver.refresh()
            time.sleep(random.uniform(2, 4))

        try:
            apenas_ativos = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH,
                                            "//label[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'apenas ativos')]/preceding-sibling::input[@type='checkbox'] | //input[@type='checkbox' and contains(@id, 'ativos')]"))
            )
            if not apenas_ativos.is_selected():
                driver.execute_script("arguments[0].click();", apenas_ativos)
                logging.info("Opção 'Apenas Ativos' marcada com sucesso!")
                print("Opção 'Apenas Ativos' marcada com sucesso!")
        except Exception as e:
            logging.warning(f"Erro ao marcar 'Apenas Ativos' (continuando sem marcar): {e}")
            print(f"Erro ao marcar 'Apenas Ativos' (continuando sem marcar): {e}")

        try:
            search_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR,
                                            'button[type="submit"], input[type="submit"], button.search-button, button.btn-primary'))
            )
            driver.execute_script("arguments[0].click();", search_button)
            time.sleep(random.uniform(3, 5))
        except Exception as e:
            logging.warning(f"Erro ao clicar no botão de pesquisa: {e}")
            print(f"Erro ao clicar no botão de pesquisa: {e}")

    except TimeoutException as e:
        logging.warning(f"Formulário de pesquisa não encontrado, prosseguindo com a coleta de dados: {e}")
        print(f"Formulário de pesquisa não encontrado, prosseguindo com a coleta de dados: {e}")

    return process_all_pages_at_once(driver, base_url,
                                     'article.search-results__article-person, .search-results article, .result-item',
                                     max_pages=100,
                                     insert_func=inserir_advogado, conn=conn)


def coletar_advogados_madeira(driver, conn):
    logging.info("Iniciando scraping de advogados...")
    print("🔎 Iniciando scraping de advogados...")
    base_url = 'https://portal.oa.pt/advogados/pesquisa-de-advogados/?l=&cg=M&ce=&n=&lo=&m=&cp=&a=on&op=&o=0&g-recaptcha-response='
    driver.get(base_url)
    try:
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'form'))
        )
        if check_captcha(driver):
            driver.refresh()
            time.sleep(random.uniform(2, 4))

        try:
            apenas_ativos = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH,
                                            "//label[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'apenas ativos')]/preceding-sibling::input[@type='checkbox'] | //input[@type='checkbox' and contains(@id, 'ativos')]"))
            )
            if not apenas_ativos.is_selected():
                driver.execute_script("arguments[0].click();", apenas_ativos)
                logging.info("Opção 'Apenas Ativos' marcada com sucesso!")
                print("Opção 'Apenas Ativos' marcada com sucesso!")
        except Exception as e:
            logging.warning(f"Erro ao marcar 'Apenas Ativos' (continuando sem marcar): {e}")
            print(f"Erro ao marcar 'Apenas Ativos' (continuando sem marcar): {e}")

        try:
            search_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR,
                                            'button[type="submit"], input[type="submit"], button.search-button, button.btn-primary'))
            )
            driver.execute_script("arguments[0].click();", search_button)
            time.sleep(random.uniform(3, 5))
        except Exception as e:
            logging.warning(f"Erro ao clicar no botão de pesquisa: {e}")
            print(f"Erro ao clicar no botão de pesquisa: {e}")

    except TimeoutException as e:
        logging.warning(f"Formulário de pesquisa não encontrado, prosseguindo com a coleta de dados: {e}")
        print(f"Formulário de pesquisa não encontrado, prosseguindo com a coleta de dados: {e}")

    return process_all_pages_at_once(driver, base_url,
                                     'article.search-results__article-person, .search-results article, .result-item',
                                     max_pages=100,
                                     insert_func=inserir_advogado, conn=conn)


def coletar_advogados_acores(driver, conn):
    logging.info("Iniciando scraping de advogados...")
    print("🔎 Iniciando scraping de advogados...")
    base_url = 'https://portal.oa.pt/advogados/pesquisa-de-advogados/?l=&cg=A&ce=&n=&lo=&m=&cp=&a=on&op=&o=0&g-recaptcha-response='
    driver.get(base_url)
    try:
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'form'))
        )
        if check_captcha(driver):
            driver.refresh()
            time.sleep(random.uniform(2, 4))

        try:
            apenas_ativos = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH,
                                            "//label[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'apenas ativos')]/preceding-sibling::input[@type='checkbox'] | //input[@type='checkbox' and contains(@id, 'ativos')]"))
            )
            if not apenas_ativos.is_selected():
                driver.execute_script("arguments[0].click();", apenas_ativos)
                logging.info("Opção 'Apenas Ativos' marcada com sucesso!")
                print("Opção 'Apenas Ativos' marcada com sucesso!")
        except Exception as e:
            logging.warning(f"Erro ao marcar 'Apenas Ativos' (continuando sem marcar): {e}")
            print(f"Erro ao marcar 'Apenas Ativos' (continuando sem marcar): {e}")

        try:
            search_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR,
                                            'button[type="submit"], input[type="submit"], button.search-button, button.btn-primary'))
            )
            driver.execute_script("arguments[0].click();", search_button)
            time.sleep(random.uniform(3, 5))
        except Exception as e:
            logging.warning(f"Erro ao clicar no botão de pesquisa: {e}")
            print(f"Erro ao clicar no botão de pesquisa: {e}")

    except TimeoutException as e:
        logging.warning(f"Formulário de pesquisa não encontrado, prosseguindo com a coleta de dados: {e}")
        print(f"Formulário de pesquisa não encontrado, prosseguindo com a coleta de dados: {e}")

    return process_all_pages_at_once(driver, base_url,  
                                     'article.search-results__article-person, .search-results article, .result-item',
                                     max_pages=100,
                                     insert_func=inserir_advogado, conn=conn)


def coletar_advogados_braga(driver, conn):
    logging.info("Iniciando scraping de advogados...")
    print("🔎 Iniciando scraping de advogados...")
    base_url = 'https://portal.oa.pt/advogados/pesquisa-de-advogados/?l=&cg=&ce=&n=&lo=BRAGA&m=&cp=&a=on&op=&o=0&g-recaptcha-response='
    driver.get(base_url)
    try:
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'form'))
        )
        if check_captcha(driver):
            driver.refresh()
            time.sleep(random.uniform(2, 4))

        try:
            apenas_ativos = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH,
                                            "//label[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'apenas ativos')]/preceding-sibling::input[@type='checkbox'] | //input[@type='checkbox' and contains(@id, 'ativos')]"))
            )
            if not apenas_ativos.is_selected():
                driver.execute_script("arguments[0].click();", apenas_ativos)
                logging.info("Opção 'Apenas Ativos' marcada com sucesso!")
                print("Opção 'Apenas Ativos' marcada com sucesso!")
        except Exception as e:
            logging.warning(f"Erro ao marcar 'Apenas Ativos' (continuando sem marcar): {e}")
            print(f"Erro ao marcar 'Apenas Ativos' (continuando sem marcar): {e}")

        try:
            search_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR,
                                            'button[type="submit"], input[type="submit"], button.search-button, button.btn-primary'))
            )
            driver.execute_script("arguments[0].click();", search_button)
            time.sleep(random.uniform(3, 5))
        except Exception as e:
            logging.warning(f"Erro ao clicar no botão de pesquisa: {e}")
            print(f"Erro ao clicar no botão de pesquisa: {e}")

    except TimeoutException as e:
        logging.warning(f"Formulário de pesquisa não encontrado, prosseguindo com a coleta de dados: {e}")
        print(f"Formulário de pesquisa não encontrado, prosseguindo com a coleta de dados: {e}")

    return process_all_pages_at_once(driver, base_url,
                                     'article.search-results__article-person, .search-results article, .result-item',
                                     max_pages=100,
                                     insert_func=inserir_advogado, conn=conn)


# Função para coletar dados de sociedades de advogados
def coletar_sociedades_lisboa(driver, conn):
    logging.info("Iniciando scraping de sociedades de advogados de lisboa...")
    print("🔎 Iniciando scraping de sociedades de advogados...")
    base_url = "https://portal.oa.pt/advogados/pesquisa-de-sociedades-de-advogados/?cg=L&r=&n=&lo=&m=&cp=&op=&o=0"
    driver.get(base_url)
    time.sleep(random.uniform(3, 5))

    try:
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'form'))
        )
        if check_captcha(driver):
            driver.refresh()
            time.sleep(random.uniform(2, 4))

        try:
            search_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR,
                                            'button[type="submit"], input[type="submit"], button.search-button, button.btn-primary'))
            )
            driver.execute_script("arguments[0].click();", search_button)
            time.sleep(random.uniform(3, 5))
        except Exception as e:
            logging.error(f"Erro ao clicar no botão de pesquisa para {base_url}: {e}")
            print(f"Erro ao clicar no botão de pesquisa para {base_url}: {e}")

    except TimeoutException as e:
        logging.warning(f"Formulário de pesquisa não encontrado, prosseguindo com a coleta de dados: {e}")
        print(f"Formulário de pesquisa não encontrado, prosseguindo com a coleta de dados: {e}")

    return process_all_pages_at_once(driver, base_url,
                                     'article.search-results__article-person, .search-results article, .result-item',
                                     max_pages=100,
                                     insert_func=inserir_sociedades, conn=conn)


def coletar_sociedades_porto(driver, conn):
    logging.info("Iniciando scraping de sociedades de advogados do porto...")
    print("🔎 Iniciando scraping de sociedades de advogados...")
    base_url = "https://portal.oa.pt/advogados/pesquisa-de-sociedades-de-advogados/?cg=P&r=&n=&lo=&m=&cp=&op=&o=0"
    driver.get(base_url)
    time.sleep(random.uniform(3, 5))

    try:
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'form'))
        )
        if check_captcha(driver):
            driver.refresh()
            time.sleep(random.uniform(2, 4))

        try:
            search_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR,
                                            'button[type="submit"], input[type="submit"], button.search-button, button.btn-primary'))
            )
            driver.execute_script("arguments[0].click();", search_button)
            time.sleep(random.uniform(3, 5))
        except Exception as e:
            logging.error(f"Erro ao clicar no botão de pesquisa para {base_url}: {e}")
            print(f"Erro ao clicar no botão de pesquisa para {base_url}: {e}")

    except TimeoutException as e:
        logging.warning(f"Formulário de pesquisa não encontrado, prosseguindo com a coleta de dados: {e}")
        print(f"Formulário de pesquisa não encontrado, prosseguindo com a coleta de dados: {e}")

    return process_all_pages_at_once(driver, base_url,
                                     'article.search-results__article-person, .search-results article, .result-item',
                                     max_pages=100,
                                     insert_func=inserir_sociedades, conn=conn)


def coletar_sociedades_coimbra(driver, conn):
    logging.info("Iniciando scraping de sociedades de advogados de coimbra...")
    print("🔎 Iniciando scraping de sociedades de advogados...")
    base_url = "https://portal.oa.pt/advogados/pesquisa-de-sociedades-de-advogados/?cg=C&r=&n=&lo=&m=&cp=&op=&o=0&page=1"
    driver.get(base_url)
    time.sleep(random.uniform(3, 5))

    try:
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'form'))
        )
        if check_captcha(driver):
            driver.refresh()
            time.sleep(random.uniform(2, 4))

        try:
            search_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR,
                                            'button[type="submit"], input[type="submit"], button.search-button, button.btn-primary'))
            )
            driver.execute_script("arguments[0].click();", search_button)
            time.sleep(random.uniform(3, 5))
        except Exception as e:
            logging.error(f"Erro ao clicar no botão de pesquisa para {base_url}: {e}")
            print(f"Erro ao clicar no botão de pesquisa para {base_url}: {e}")

    except TimeoutException as e:
        logging.warning(f"Formulário de pesquisa não encontrado, prosseguindo com a coleta de dados: {e}")
        print(f"Formulário de pesquisa não encontrado, prosseguindo com a coleta de dados: {e}")

    return process_all_pages_at_once(driver, base_url,
                                     'article.search-results__article-person, .search-results article, .result-item',
                                     max_pages=100,
                                     insert_func=inserir_sociedades, conn=conn)


def coletar_sociedades_evora(driver, conn):
    logging.info("Iniciando scraping de sociedades de advogados de evora...")
    print("🔎 Iniciando scraping de sociedades de advogados...")
    base_url = "https://portal.oa.pt/advogados/pesquisa-de-sociedades-de-advogados/?cg=E&r=&n=&lo=&m=&cp=&op=&o=0"
    driver.get(base_url)
    time.sleep(random.uniform(3, 5))

    try:
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'form'))
        )
        if check_captcha(driver):
            driver.refresh()
            time.sleep(random.uniform(2, 4))

        try:
            search_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR,
                                            'button[type="submit"], input[type="submit"], button.search-button, button.btn-primary'))
            )
            driver.execute_script("arguments[0].click();", search_button)
            time.sleep(random.uniform(3, 5))
        except Exception as e:
            logging.error(f"Erro ao clicar no botão de pesquisa para {base_url}: {e}")
            print(f"Erro ao clicar no botão de pesquisa para {base_url}: {e}")

    except TimeoutException as e:
        logging.warning(f"Formulário de pesquisa não encontrado, prosseguindo com a coleta de dados: {e}")
        print(f"Formulário de pesquisa não encontrado, prosseguindo com a coleta de dados: {e}")

    return process_all_pages_at_once(driver, base_url,
                                     'article.search-results__article-person, .search-results article, .result-item',
                                     max_pages=100,
                                     insert_func=inserir_sociedades, conn=conn)


def coletar_sociedades_faro(driver, conn):
    logging.info("Iniciando scraping de sociedades de advogados de faro...")
    print("🔎 Iniciando scraping de sociedades de advogados...")
    base_url = "https://portal.oa.pt/advogados/pesquisa-de-sociedades-de-advogados/?cg=F&r=&n=&lo=&m=&cp=&op=&o=0"
    driver.get(base_url)
    time.sleep(random.uniform(3, 5))

    try:
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'form'))
        )
        if check_captcha(driver):
            driver.refresh()
            time.sleep(random.uniform(2, 4))

        try:
            search_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR,
                                            'button[type="submit"], input[type="submit"], button.search-button, button.btn-primary'))
            )
            driver.execute_script("arguments[0].click();", search_button)
            time.sleep(random.uniform(3, 5))
        except Exception as e:
            logging.error(f"Erro ao clicar no botão de pesquisa para {base_url}: {e}")
            print(f"Erro ao clicar no botão de pesquisa para {base_url}: {e}")

    except TimeoutException as e:
        logging.warning(f"Formulário de pesquisa não encontrado, prosseguindo com a coleta de dados: {e}")
        print(f"Formulário de pesquisa não encontrado, prosseguindo com a coleta de dados: {e}")

    return process_all_pages_at_once(driver, base_url,
                                     'article.search-results__article-person, .search-results article, .result-item',
                                     max_pages=100,
                                     insert_func=inserir_sociedades, conn=conn)


def coletar_sociedades_madeira(driver, conn):
    logging.info("Iniciando scraping de sociedades de advogados da madeira...")
    print("🔎 Iniciando scraping de sociedades de advogados...")
    base_url = "https://portal.oa.pt/advogados/pesquisa-de-sociedades-de-advogados/?cg=M&r=&n=&lo=&m=&cp=&op=&o=0"
    driver.get(base_url)
    time.sleep(random.uniform(3, 5))

    try:
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'form'))
        )
        if check_captcha(driver):
            driver.refresh()
            time.sleep(random.uniform(2, 4))

        try:
            search_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR,
                                            'button[type="submit"], input[type="submit"], button.search-button, button.btn-primary'))
            )
            driver.execute_script("arguments[0].click();", search_button)
            time.sleep(random.uniform(3, 5))
        except Exception as e:
            logging.error(f"Erro ao clicar no botão de pesquisa para {base_url}: {e}")
            print(f"Erro ao clicar no botão de pesquisa para {base_url}: {e}")

    except TimeoutException as e:
        logging.warning(f"Formulário de pesquisa não encontrado, prosseguindo com a coleta de dados: {e}")
        print(f"Formulário de pesquisa não encontrado, prosseguindo com a coleta de dados: {e}")

    return process_all_pages_at_once(driver, base_url,
                                     'article.search-results__article-person, .search-results article, .result-item',
                                     max_pages=100,
                                     insert_func=inserir_sociedades, conn=conn)


def coletar_sociedades_acores(driver, conn):
    logging.info("Iniciando scraping de sociedades de advogados da madeira...")
    print("🔎 Iniciando scraping de sociedades de advogados...")
    base_url = "https://portal.oa.pt/advogados/pesquisa-de-sociedades-de-advogados/?cg=A&r=&n=&lo=&m=&cp=&op=&o=0"
    driver.get(base_url)
    time.sleep(random.uniform(3, 5))

    try:
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'form'))
        )
        if check_captcha(driver):
            driver.refresh()
            time.sleep(random.uniform(2, 4))

        try:
            search_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR,
                                            'button[type="submit"], input[type="submit"], button.search-button, button.btn-primary'))
            )
            driver.execute_script("arguments[0].click();", search_button)
            time.sleep(random.uniform(3, 5))
        except Exception as e:
            logging.error(f"Erro ao clicar no botão de pesquisa para {base_url}: {e}")
            print(f"Erro ao clicar no botão de pesquisa para {base_url}: {e}")

    except TimeoutException as e:
        logging.warning(f"Formulário de pesquisa não encontrado, prosseguindo com a coleta de dados: {e}")
        print(f"Formulário de pesquisa não encontrado, prosseguindo com a coleta de dados: {e}")

    return process_all_pages_at_once(driver, base_url,
                                     'article.search-results__article-person, .search-results article, .result-item',
                                     max_pages=100,
                                     insert_func=inserir_sociedades, conn=conn)


def coletar_sociedades_braga(driver, conn):
    logging.info("Iniciando scraping de sociedades de advogados da madeira...")
    print("🔎 Iniciando scraping de sociedades de advogados...")
    base_url = "https://portal.oa.pt/advogados/pesquisa-de-sociedades-de-advogados/?cg=&r=&n=&lo=BRAGA&m=&cp=&op=&o=0"
    driver.get(base_url)
    time.sleep(random.uniform(3, 5))

    try:
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'form'))
        )
        if check_captcha(driver):
            driver.refresh()
            time.sleep(random.uniform(2, 4))

        try:
            search_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR,
                                            'button[type="submit"], input[type="submit"], button.search-button, button.btn-primary'))
            )
            driver.execute_script("arguments[0].click();", search_button)
            time.sleep(random.uniform(3, 5))
        except Exception as e:
            logging.error(f"Erro ao clicar no botão de pesquisa para {base_url}: {e}")
            print(f"Erro ao clicar no botão de pesquisa para {base_url}: {e}")

    except TimeoutException as e:
        logging.warning(f"Formulário de pesquisa não encontrado, prosseguindo com a coleta de dados: {e}")
        print(f"Formulário de pesquisa não encontrado, prosseguindo com a coleta de dados: {e}")

    return process_all_pages_at_once(driver, base_url,
                                     'article.search-results__article-person, .search-results article, .result-item',
                                     max_pages=100,
                                     insert_func=inserir_sociedades, conn=conn)


# Função para coletar dados de advogados estagiários
def coletar_estagiarios_lisboa(driver, conn):
    logging.info("Iniciando scraping de estagiários...")
    print("🔎 Iniciando scraping de estagiários...")
    base_url = "https://portal.oa.pt/advogados/pesquisa-de-advogados-estagiarios/?cg=L&ce=&n=&lo=&m=&cp=&a=on&op=&o=0"
    driver.get(base_url)
    time.sleep(random.uniform(3, 5))

    try:
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'form'))
        )
        if check_captcha(driver):
            driver.refresh()
            time.sleep(random.uniform(2, 4))

        try:
            search_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR,
                                            'button[type="submit"], input[type="submit"], button.search-button, button.btn-primary'))
            )
            driver.execute_script("arguments[0].click();", search_button)
            time.sleep(random.uniform(3, 5))
        except Exception as e:
            logging.error(f"Erro ao clicar no botão de pesquisa para {base_url}: {e}")
            print(f"Erro ao clicar no botão de pesquisa para {base_url}: {e}")

    except TimeoutException as e:
        logging.warning(f"Formulário de pesquisa não encontrado, prosseguindo com a coleta de dados: {e}")
        print(f"Formulário de pesquisa não encontrado, prosseguindo com a coleta de dados: {e}")

    return process_all_pages_at_once(driver, base_url,
                                     'article.search-results__article-person, .search-results article, .result-item',
                                     max_pages=100,
                                     insert_func=inserir_estagiario, conn=conn)


def coletar_estagiarios_porto(driver, conn):
    logging.info("Iniciando scraping de estagiários...")
    print("🔎 Iniciando scraping de estagiários...")
    base_url = "https://portal.oa.pt/advogados/pesquisa-de-advogados-estagiarios/?cg=P&ce=&n=&lo=&m=&cp=&a=on&op=&o=0"
    driver.get(base_url)
    time.sleep(random.uniform(3, 5))

    try:
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'form'))
        )
        if check_captcha(driver):
            driver.refresh()
            time.sleep(random.uniform(2, 4))

        try:
            search_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR,
                                            'button[type="submit"], input[type="submit"], button.search-button, button.btn-primary'))
            )
            driver.execute_script("arguments[0].click();", search_button)
            time.sleep(random.uniform(3, 5))
        except Exception as e:
            logging.error(f"Erro ao clicar no botão de pesquisa para {base_url}: {e}")
            print(f"Erro ao clicar no botão de pesquisa para {base_url}: {e}")

    except TimeoutException as e:
        logging.warning(f"Formulário de pesquisa não encontrado, prosseguindo com a coleta de dados: {e}")
        print(f"Formulário de pesquisa não encontrado, prosseguindo com a coleta de dados: {e}")

    return process_all_pages_at_once(driver, base_url,
                                     'article.search-results__article-person, .search-results article, .result-item',
                                     max_pages=100,
                                     insert_func=inserir_estagiario, conn=conn)


def coletar_estagiarios_coimbra(driver, conn):
    logging.info("Iniciando scraping de estagiários...")
    print("🔎 Iniciando scraping de estagiários...")
    base_url = "https://portal.oa.pt/advogados/pesquisa-de-advogados-estagiarios/?cg=C&ce=&n=&lo=&m=&cp=&a=on&op=&o=0"
    driver.get(base_url)
    time.sleep(random.uniform(3, 5))

    try:
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'form'))
        )
        if check_captcha(driver):
            driver.refresh()
            time.sleep(random.uniform(2, 4))

        try:
            search_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR,
                                            'button[type="submit"], input[type="submit"], button.search-button, button.btn-primary'))
            )
            driver.execute_script("arguments[0].click();", search_button)
            time.sleep(random.uniform(3, 5))
        except Exception as e:
            logging.error(f"Erro ao clicar no botão de pesquisa para {base_url}: {e}")
            print(f"Erro ao clicar no botão de pesquisa para {base_url}: {e}")

    except TimeoutException as e:
        logging.warning(f"Formulário de pesquisa não encontrado, prosseguindo com a coleta de dados: {e}")
        print(f"Formulário de pesquisa não encontrado, prosseguindo com a coleta de dados: {e}")

    return process_all_pages_at_once(driver, base_url,
                                     'article.search-results__article-person, .search-results article, .result-item',
                                     max_pages=100,
                                     insert_func=inserir_estagiario, conn=conn)


def coletar_estagiarios_evora(driver, conn):
    logging.info("Iniciando scraping de estagiários...")
    print("🔎 Iniciando scraping de estagiários...")
    base_url = "https://portal.oa.pt/advogados/pesquisa-de-advogados-estagiarios/?cg=E&ce=&n=&lo=&m=&cp=&a=on&op=&o=0"
    driver.get(base_url)
    time.sleep(random.uniform(3, 5))

    try:
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'form'))
        )
        if check_captcha(driver):
            driver.refresh()
            time.sleep(random.uniform(2, 4))

        try:
            search_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR,
                                            'button[type="submit"], input[type="submit"], button.search-button, button.btn-primary'))
            )
            driver.execute_script("arguments[0].click();", search_button)
            time.sleep(random.uniform(3, 5))
        except Exception as e:
            logging.error(f"Erro ao clicar no botão de pesquisa para {base_url}: {e}")
            print(f"Erro ao clicar no botão de pesquisa para {base_url}: {e}")

    except TimeoutException as e:
        logging.warning(f"Formulário de pesquisa não encontrado, prosseguindo com a coleta de dados: {e}")
        print(f"Formulário de pesquisa não encontrado, prosseguindo com a coleta de dados: {e}")

    return process_all_pages_at_once(driver, base_url,
                                     'article.search-results__article-person, .search-results article, .result-item',
                                     max_pages=100,
                                     insert_func=inserir_estagiario, conn=conn)


def coletar_estagiarios_faro(driver, conn):
    logging.info("Iniciando scraping de estagiários...")
    print("🔎 Iniciando scraping de estagiários...")
    base_url = "https://portal.oa.pt/advogados/pesquisa-de-advogados-estagiarios/?cg=F&ce=&n=&lo=&m=&cp=&a=on&op=&o=0"
    driver.get(base_url)
    time.sleep(random.uniform(3, 5))

    try:
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'form'))
        )
        if check_captcha(driver):
            driver.refresh()
            time.sleep(random.uniform(2, 4))

        try:
            search_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR,
                                            'button[type="submit"], input[type="submit"], button.search-button, button.btn-primary'))
            )
            driver.execute_script("arguments[0].click();", search_button)
            time.sleep(random.uniform(3, 5))
        except Exception as e:
            logging.error(f"Erro ao clicar no botão de pesquisa para {base_url}: {e}")
            print(f"Erro ao clicar no botão de pesquisa para {base_url}: {e}")

    except TimeoutException as e:
        logging.warning(f"Formulário de pesquisa não encontrado, prosseguindo com a coleta de dados: {e}")
        print(f"Formulário de pesquisa não encontrado, prosseguindo com a coleta de dados: {e}")

    return process_all_pages_at_once(driver, base_url,
                                     'article.search-results__article-person, .search-results article, .result-item',
                                     max_pages=100,
                                     insert_func=inserir_estagiario, conn=conn)


def coletar_estagiarios_madeira(driver, conn):
    logging.info("Iniciando scraping de estagiários...")
    print("🔎 Iniciando scraping de estagiários...")
    base_url = "https://portal.oa.pt/advogados/pesquisa-de-advogados-estagiarios/?cg=M&ce=&n=&lo=&m=&cp=&a=on&op=&o=0"
    driver.get(base_url)
    time.sleep(random.uniform(3, 5))

    try:
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'form'))
        )
        if check_captcha(driver):
            driver.refresh()
            time.sleep(random.uniform(2, 4))

        try:
            search_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR,
                                            'button[type="submit"], input[type="submit"], button.search-button, button.btn-primary'))
            )
            driver.execute_script("arguments[0].click();", search_button)
            time.sleep(random.uniform(3, 5))
        except Exception as e:
            logging.error(f"Erro ao clicar no botão de pesquisa para {base_url}: {e}")
            print(f"Erro ao clicar no botão de pesquisa para {base_url}: {e}")

    except TimeoutException as e:
        logging.warning(f"Formulário de pesquisa não encontrado, prosseguindo com a coleta de dados: {e}")
        print(f"Formulário de pesquisa não encontrado, prosseguindo com a coleta de dados: {e}")

    return process_all_pages_at_once(driver, base_url,
                                     'article.search-results__article-person, .search-results article, .result-item',
                                     max_pages=100,
                                     insert_func=inserir_estagiario, conn=conn)


def coletar_estagiarios_acores(driver, conn):
    logging.info("Iniciando scraping de estagiários...")
    print("🔎 Iniciando scraping de estagiários...")
    base_url = "https://portal.oa.pt/advogados/pesquisa-de-advogados-estagiarios/?cg=A&ce=&n=&lo=&m=&cp=&op=&o=0"
    driver.get(base_url)
    time.sleep(random.uniform(3, 5))

    try:
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'form'))
        )
        if check_captcha(driver):
            driver.refresh()
            time.sleep(random.uniform(2, 4))

        try:
            search_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR,
                                            'button[type="submit"], input[type="submit"], button.search-button, button.btn-primary'))
            )
            driver.execute_script("arguments[0].click();", search_button)
            time.sleep(random.uniform(3, 5))
        except Exception as e:
            logging.error(f"Erro ao clicar no botão de pesquisa para {base_url}: {e}")
            print(f"Erro ao clicar no botão de pesquisa para {base_url}: {e}")

    except TimeoutException as e:
        logging.warning(f"Formulário de pesquisa não encontrado, prosseguindo com a coleta de dados: {e}")
        print(f"Formulário de pesquisa não encontrado, prosseguindo com a coleta de dados: {e}")

    return process_all_pages_at_once(driver, base_url,
                                     'article.search-results__article-person, .search-results article, .result-item',
                                     max_pages=100,
                                     insert_func=inserir_estagiario, conn=conn)


def coletar_estagiarios_braga(driver, conn):
    logging.info("Iniciando scraping de estagiários...")
    print("🔎 Iniciando scraping de estagiários...")
    base_url = "https://portal.oa.pt/advogados/pesquisa-de-advogados-estagiarios/?cg=&ce=&n=&lo=BRAGA&m=&cp=&op=&o=0"
    driver.get(base_url)
    time.sleep(random.uniform(3, 5))

    try:
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'form'))
        )
        if check_captcha(driver):
            driver.refresh()
            time.sleep(random.uniform(2, 4))

        try:
            search_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR,
                                            'button[type="submit"], input[type="submit"], button.search-button, button.btn-primary'))
            )
            driver.execute_script("arguments[0].click();", search_button)
            time.sleep(random.uniform(3, 5))
        except Exception as e:
            logging.error(f"Erro ao clicar no botão de pesquisa para {base_url}: {e}")
            print(f"Erro ao clicar no botão de pesquisa para {base_url}: {e}")

    except TimeoutException as e:
        logging.warning(f"Formulário de pesquisa não encontrado, prosseguindo com a coleta de dados: {e}")
        print(f"Formulário de pesquisa não encontrado, prosseguindo com a coleta de dados: {e}")

    return process_all_pages_at_once(driver, base_url,
                                     'article.search-results__article-person, .search-results article, .result-item',
                                     max_pages=100,
                                     insert_func=inserir_estagiario, conn=conn)


# Função para coletar dados de tribunais
def coletar_tribunais(driver, conn):
    logging.info("Iniciando scraping de tribunais...")
    print("🔎 Iniciando scraping de tribunais...")
    base_url = "https://www.citius.mj.pt/portal/contactostribunais.aspx"
    all_data = []

    # Carrega a página uma vez para pegar as localidades
    driver.get(base_url)
    time.sleep(2)
    select_region_initial = WebDriverWait(driver, 30).until(
        EC.element_to_be_clickable((By.CSS_SELECTOR, 'select[name*="ddlLocalidade"], select[id*="ddlLocalidade"]'))
    )
    options = select_region_initial.find_elements(By.TAG_NAME, 'option')
    region_values = [option.get_attribute('value') for option in options if
                     option.get_attribute('value') and option.get_attribute('value') != '0']

    for region_value in region_values:
        try:
            print(f"🔄 Tentando selecionar localidade: {region_value}")
            driver.get(base_url)
            time.sleep(2)
            select_element = WebDriverWait(driver, 30).until(
                EC.element_to_be_clickable(
                    (By.CSS_SELECTOR, 'select[name*="ddlLocalidade"], select[id*="ddlLocalidade"]'))
            )
            dropdown = Select(select_element)
            dropdown.select_by_value(region_value)
            time.sleep(2)  # Aguarda o postback

            # Aguarda os resultados aparecerem
            WebDriverWait(driver, 30).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, '.pesquisaresultado'))
            )

            html = driver.page_source
            soup = BeautifulSoup(html, 'lxml')
            resultados = soup.find_all('div', class_='pesquisaresultado')
            print(f"Encontrados {len(resultados)} blocos de tribunal na localidade {region_value}.")

            for resultado in resultados:
                try:
                    nome = resultado.find('h1').text.strip() if resultado.find('h1') else 'N/D'
                    morada = 'N/D'
                    morada_element = resultado.find('strong', string='Morada: ')
                    if morada_element:
                        morada_parts = []
                        for sibling in morada_element.next_siblings:
                            if getattr(sibling, 'name', None) == 'br':
                                break
                            if isinstance(sibling, str):
                                morada_parts.append(sibling.strip())
                        morada = ' '.join([p for p in morada_parts if p]).replace('\xa0', ' ').replace(' ,', ',')
                    telefone = 'N/D'
                    telefone_element = resultado.find('strong', string='Telefone: ')
                    if telefone_element:
                        telefone_parts = []
                        for sibling in telefone_element.next_siblings:
                            if getattr(sibling, 'name', None) == 'br':
                                break
                            if isinstance(sibling, str):
                                telefone_parts.append(sibling.strip())
                        telefone = ' '.join([p for p in telefone_parts if p])
                    email_element = resultado.find('strong', string='Correio Electrónico: ')
                    email = email_element.find_next('a').text.strip() if email_element and email_element.find_next(
                        'a') else 'N/D'
                    tribunal_data = {
                        'Nome': nome,
                        'Morada': morada,
                        'Telefone': telefone,
                        'Email': email,
                        'Tipo': 'tribunal'
                    }
                    if conn and conn.is_connected():
                        inserir_tribunal(conn, tribunal_data['Nome'], tribunal_data['Morada'],
                                         tribunal_data['Telefone'], tribunal_data['Email'])
                    all_data.append(tribunal_data)
                    logging.info(f"Tribunal {tribunal_data['Nome']} processado com sucesso!")
                    print(f"✅ Tribunal {tribunal_data['Nome']} processado com sucesso!")
                except Exception as e:
                    logging.error(f"Erro ao processar tribunal: {e}")
                    print(f"Erro ao processar tribunal: {e}")
                    continue
        except Exception as e:
            print(f"Erro ao selecionar localidade {region_value}: {e}")
            continue

    if all_data:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"dados_tribunais_{timestamp}.csv"
        save_to_csv(all_data, filename)
        logging.info(f"Dados salvos em CSV: {filename}")
        print(f"✅ Dados salvos em CSV: {filename}")
        return all_data


def coletar_julgados(driver, conn):
    logging.info("Iniciando scraping de julgados de paz detalhados...")
    print("🔎 Iniciando scraping de julgados de paz detalhados...")
    base_url = "https://dgpj.justica.gov.pt/resolucao-de-litigios/julgados-de-paz/encontrar-um-julgado-de-paz"
    all_data = []

    driver.get(base_url)
    time.sleep(2)

    # Encontra todos os links de agrupamentos (títulos clicáveis)
    agrupamentos_links = driver.find_elements(By.CSS_SELECTOR, "a[id^='dnn_ctr19541_FAQs_lstFAQs_Q2_']")
    print(f"Encontrados {len(agrupamentos_links)} agrupamentos de julgados de paz.")

    for i, link in enumerate(agrupamentos_links):
        try:
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", link)
            link.click()
            time.sleep(2)  # Aguarda expandir

            # Após expandir, pega o painel de conteúdo expandido relacionado a este agrupamento
            panel_id = link.get_attribute("id").replace("Q2", "pnl")
            panel = driver.find_element(By.ID, panel_id)
            detalhes_html = panel.get_attribute("innerHTML")
            detalhes_soup = BeautifulSoup(detalhes_html, 'lxml')

            # Agora, use o mesmo parsing que já fazia para blocos de detalhes
            blocos = detalhes_soup.find_all('div', class_='Normal')
            nome_agrup = link.text.strip()
            for bloco in blocos:
                ps = bloco.find_all('p')
                localidade = None
                morada = []
                telefone = ''
                email = ''
                for p in ps:
                    txt = p.get_text(strip=True)
                    if txt.startswith('Telefone'):
                        telefone = txt.replace('Telefone:', '').split('-')[0].strip()
                    elif txt.startswith('Email'):
                        email_tag = p.find('a')
                        email = email_tag.text.strip() if email_tag else ''
                    elif txt and not txt.startswith('Fax') and not txt.startswith('Não instalados'):
                        if p.find('strong'):
                            if localidade and morada:
                                tribunal_data = {
                                    'Nome': f"{nome_agrup} - {localidade}",
                                    'Morada': ' '.join(morada),
                                    'Telefone': telefone,
                                    'Email': email,
                                    'Tipo': 'julgado_paz'
                                }
                                if conn and conn.is_connected():
                                    inserir_tribunal(conn, tribunal_data['Nome'], tribunal_data['Morada'],
                                                     tribunal_data['Telefone'], tribunal_data['Email'])
                                all_data.append(tribunal_data)
                                print(f"✅ Julgado de Paz {tribunal_data['Nome']} processado com sucesso!")
                            localidade = txt
                            morada = []
                            telefone = ''
                            email = ''
                        else:
                            morada.append(txt)
            if localidade and morada:
                tribunal_data = {
                    'Nome': f"{nome_agrup} - {localidade}",
                    'Morada': ' '.join(morada),
                    'Telefone': telefone,
                    'Email': email,
                    'Tipo': 'julgado_paz'
                }
                if conn and conn.is_connected():
                    inserir_tribunal(conn, tribunal_data['Nome'], tribunal_data['Morada'], tribunal_data['Telefone'],
                                     tribunal_data['Email'])
                all_data.append(tribunal_data)
                print(f"✅ Julgado de Paz {tribunal_data['Nome']} processado com sucesso!")
        except Exception as e:
            print(f"Erro ao processar agrupamento {i}: {e}")
            continue

    # Salva e retorna os dados como já faz
    if all_data:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        save_to_csv(all_data, f"julgados_{timestamp}.csv")

    print(f"✅ Total de julgados de paz coletados: {len(all_data)}")
    time.sleep(2)  # Pequeno delay para visualizar a mensagem
    return all_data


# Função auxiliar para extrair dados de agentes de execução
def extract_osae_data(item, base_url, tipo):
    """
    Extrai dados de um agente de execução do site da OSAE.

    Args:
        item: Elemento BeautifulSoup contendo os dados do agente
        base_url: URL base do site
        tipo: Tipo de registro

    Returns:
        dict: Dicionário com os dados extraídos
    """
    item_data = {}

    # Extrai o nome
    nome_tag = item.find(['h4', 'h5', 'strong', '.nome', '.name'])
    nome = nome_tag.get_text(strip=True) if nome_tag else 'N/D'
    item_data['Nome'] = nome

    # Inicializa outros campos
    item_data['Situação'] = 'N/D'
    item_data['Cédula'] = 'N/D'
    item_data['Localidade'] = 'N/D'
    item_data['Telefone'] = 'N/D'
    item_data['Email'] = 'N/D'
    item_data['Tipo'] = tipo

    # Procura por informações em listas e parágrafos
    for element in item.find_all(['p', 'li', 'div']):
        text = element.get_text(strip=True)

        # Extrai situação
        if 'Situação:' in text:
            item_data['Situação'] = text.replace('Situação:', '').strip()

        # Extrai cédula
        elif 'Cédula:' in text:
            item_data['Cédula'] = text.replace('Cédula:', '').strip()

        # Extrai localidade
        elif 'Localidade:' in text:
            item_data['Localidade'] = text.replace('Localidade:', '').strip()

        # Extrai contatos
        elif 'Contactos:' in text:
            contatos = text.replace('Contactos:', '').strip()
            if '-' in contatos:
                telefone, email = contatos.split('-', 1)
                item_data['Telefone'] = telefone.strip()
                item_data['Email'] = email.strip()
            else:
                item_data['Telefone'] = contatos.strip()

        # Tenta extrair email se estiver em um link
        elif element.find('a', href=lambda x: x and 'mailto:' in x):
            email_link = element.find('a', href=lambda x: x and 'mailto:' in x)
            item_data['Email'] = email_link['href'].replace('mailto:', '').strip()

        # Tenta extrair telefone se estiver em um link
        elif element.find('a', href=lambda x: x and 'tel:' in x):
            tel_link = element.find('a', href=lambda x: x and 'tel:' in x)
            item_data['Telefone'] = tel_link['href'].replace('tel:', '').strip()

    return item_data


def scrape_osae(driver=None, conn=None):
    """
    Função para coletar dados dos Agentes de Execução do site OSAE.
    Abre o navegador, faz a pesquisa e retorna os dados coletados.

    Args:
        driver: WebDriver opcional. Se não fornecido, será criado um novo.
        conn: Conexão com banco de dados opcional.
    """
    logging.info("Iniciando scraping de agentes de execução...")
    print("🔎 Iniciando scraping de agentes de execução...")
    base_url = "https://osae.pt/pt/pesquisas/1/1/1"

    try:
        # Acessa a página
        print("🌐 Acessando site OSAE...")
        driver.get(base_url)
        time.sleep(5)  # Aguarda carregamento inicial

        # Espera a página carregar completamente
        print("⏳ Aguardando carregamento da página...")
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "form, .search-form, #ContentPlaceHolder1_RadioPesquisa2"))
        )
        time.sleep(3)

        # Seleciona Agentes de Execução
        print("📋 Selecionando Agentes de Execução...")
        try:
            radio = WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.ID, "ContentPlaceHolder1_RadioPesquisa2"))
            )
            if not radio.is_selected():
                driver.execute_script("arguments[0].click();", radio)
                time.sleep(2)
                logging.info("Opção 'Agentes de Execução' selecionada com sucesso!")
                print("✅ Opção 'Agentes de Execução' selecionada com sucesso!")
        except Exception as e:
            logging.warning(f"Erro ao selecionar 'Agentes de Execução': {e}")
            print(f"⚠️ Erro ao selecionar 'Agentes de Execução': {e}")
            try:
                radio = driver.find_element(By.CSS_SELECTOR, "input[type='radio'][value='2']")
                if not radio.is_selected():
                    driver.execute_script("arguments[0].click();", radio)
                    time.sleep(2)
            except Exception as e2:
                logging.error(f"Método alternativo falhou: {e2}")
                print(f"❌ Método alternativo falhou: {e2}")
                return []

        # Marca checkbox Ativos (se existir)
        print("✅ Tentando marcar opção Ativas (se existir)...")
        try:
            checkbox = driver.find_element(By.CSS_SELECTOR, "input[type='checkbox'][id*='cb2']")
            if not checkbox.is_selected():
                driver.execute_script("arguments[0].click();", checkbox)
                time.sleep(2)
            logging.info("Opção 'Ativos' marcada com sucesso!")
            print("✅ Opção 'Ativos' marcada com sucesso!")
        except Exception as e:
            logging.warning(f"Checkbox 'Ativos' não encontrada ou não foi possível marcar: {e}")
            print(f"⚠️ Checkbox 'Ativos' não encontrada ou não foi possível marcar: {e}")

        # Clica em pesquisar
        print("🔍 Realizando pesquisa...")
        try:
            search_button = WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.ID, "ContentPlaceHolder1_bt1"))
            )
            driver.execute_script("arguments[0].click();", search_button)
            time.sleep(5)
            logging.info("Pesquisa iniciada com sucesso!")
            print("✅ Pesquisa iniciada com sucesso!")
        except Exception as e:
            logging.warning(f"Erro ao clicar no botão de pesquisa: {e}")
            print(f"⚠️ Erro ao clicar no botão de pesquisa: {e}")
            try:
                search_button = driver.find_element(By.CSS_SELECTOR, "input[type='submit'][value*='Pesquisar']")
                driver.execute_script("arguments[0].click();", search_button)
                time.sleep(5)
            except Exception as e2:
                logging.error(f"Método alternativo falhou: {e2}")
                print(f"❌ Método alternativo falhou: {e2}")
                return []

        # Aguarda os resultados
        print("⏳ Aguardando resultados...")
        time.sleep(5)

        # Extrai os dados da página atual (única página)
        html = driver.page_source
        soup = BeautifulSoup(html, 'lxml')
        items = soup.select("div.solicitador")

        if not items:
            print("Nenhum item encontrado na página.")
        else:
            print(f"Encontrados {len(items)} itens na página.")
            all_data = []
            for item in items:
                try:
                    item_data = extract_osae_data(item, base_url, 'agente_execucao')
                    if conn and conn.is_connected():
                        inserir_agente_execucao(
                            conn,
                            item_data.get('Nome', 'N/D'),
                            item_data.get('Situação', 'N/D'),
                            item_data.get('Cédula', 'N/D'),
                            item_data.get('Localidade', 'N/D'),
                            item_data.get('Telefone', 'N/D'),
                            item_data.get('Email', 'N/D'),
                            item_data.get('Tipo', 'N/D')
                        )
                    all_data.append(item_data)
                    print(f"✅ Agente de Execução {item_data['Nome']} processado com sucesso!")
                except Exception as e:
                    logging.error(f"Erro ao processar agente de execução: {e}")
                    print(f"❌ Erro ao processar agente de execução: {e}")
            if all_data:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"dados_agentes_execucao_{timestamp}.csv"
                save_to_csv(all_data, filename)
                print(f"✅ Dados salvos em CSV: {filename}")
            print(f"✅ Total de agentes de execução coletados: {len(all_data)}")
            return all_data

    except Exception as e:
        logging.error(f"Erro durante o scraping: {str(e)}")
        print(f"❌ Erro durante o scraping: {str(e)}")
        error_page = f"error_page_osae_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
        with open(error_page, 'w', encoding='utf-8') as f:
            f.write(driver.page_source)
        print(f"📝 Página de erro salva em: {error_page}")
        return []


def scrape_osae_sociedades(driver=None, conn=None):
    """
    Função para coletar dados das Sociedades de Execução do site OSAE.
    Abre o navegador, faz a pesquisa e retorna os dados coletados.
    """
    logging.info("Iniciando scraping de sociedades de execução...")
    print("🔎 Iniciando scraping de sociedades de execução...")
    base_url = "https://osae.pt/pt/pesquisas/1/1/2"
    all_data = []
    try:
        # Acessa a página
        print("🌐 Acessando site OSAE (Sociedades)...")
        driver.get(base_url)
        time.sleep(5)

        # Espera a página carregar completamente
        print("⏳ Aguardando carregamento da página...")
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "form, .search-form, #ContentPlaceHolder1_RadioPesquisa3"))
        )
        time.sleep(3)

        # Seleciona Sociedades de Execução
        print("📋 Selecionando Sociedades de Execução...")
        try:
            radio = WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.ID, "ContentPlaceHolder1_RadioPesquisa3"))
            )
            if not radio.is_selected():
                driver.execute_script("arguments[0].click();", radio)
                time.sleep(2)
                logging.info("Opção 'Sociedades de Execução' selecionada com sucesso!")
                print("✅ Opção 'Sociedades de Execução' selecionada com sucesso!")
        except Exception as e:
            logging.warning(f"Erro ao selecionar 'Sociedades de Execução': {e}")
            print(f"⚠️ Erro ao selecionar 'Sociedades de Execução': {e}")
            try:
                radio = driver.find_element(By.CSS_SELECTOR, "input[type='radio'][value='3']")
                if not radio.is_selected():
                    driver.execute_script("arguments[0].click();", radio)
                    time.sleep(2)
            except Exception as e2:
                logging.error(f"Método alternativo falhou: {e2}")
                print(f"❌ Método alternativo falhou: {e2}")
                return []

        # Marca checkbox Ativos (se existir)
        print("✅ Tentando marcar opção Ativas (se existir)...")
        try:
            checkbox = driver.find_element(By.CSS_SELECTOR, "input[type='checkbox'][id*='cb2']")
            if not checkbox.is_selected():
                driver.execute_script("arguments[0].click();", checkbox)
                time.sleep(2)
            logging.info("Opção 'Ativos' marcada com sucesso!")
            print("✅ Opção 'Ativos' marcada com sucesso!")
        except Exception as e:
            logging.warning(f"Checkbox 'Ativos' não encontrada ou não foi possível marcar: {e}")
            print(f"⚠️ Checkbox 'Ativos' não encontrada ou não foi possível marcar: {e}")

        # Clica em pesquisar
        print("🔍 Realizando pesquisa...")
        try:
            search_button = WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.ID, "ContentPlaceHolder1_bt1"))
            )
            driver.execute_script("arguments[0].click();", search_button)
            time.sleep(5)
            logging.info("Pesquisa iniciada com sucesso!")
            print("✅ Pesquisa iniciada com sucesso!")
        except Exception as e:
            logging.warning(f"Erro ao clicar no botão de pesquisa: {e}")
            print(f"⚠️ Erro ao clicar no botão de pesquisa: {e}")
            try:
                search_button = driver.find_element(By.CSS_SELECTOR, "input[type='submit'][value*='Pesquisar']")
                driver.execute_script("arguments[0].click();", search_button)
                time.sleep(5)
            except Exception as e2:
                logging.error(f"Método alternativo falhou: {e2}")
                print(f"❌ Método alternativo falhou: {e2}")
                return []

        # Aguarda os resultados
        print("⏳ Aguardando resultados...")
        time.sleep(5)

        # Extrai os dados da página atual (única página)
        html = driver.page_source
        soup = BeautifulSoup(html, 'lxml')
        items = soup.select("div.solicitador")

        if not items:
            print("Nenhum item encontrado na página.")
        else:
            print(f"Encontrados {len(items)} itens na página.")
            for item in items:
                try:
                    item_data = extract_osae_data(item, base_url, 'sociedade_execucao')
                    if conn and conn.is_connected():
                        inserir_sociedades(
                            conn,
                            item_data.get('Nome', 'N/D'),  # name
                            'N/D',  # conselho_regional
                            'N/D',  # morada
                            item_data.get('Situação', 'N/D'),  # estado
                            item_data.get('Telefone', 'N/D'),
                            item_data.get('Email', 'N/D'),
                            base_url,  # site
                            'sociedade_execucao',  # tipo
                            item_data.get('Localidade', 'N/D'),
                            'N/D',  # registo
                            'N/D',  # codigo_postal
                            'N/D',  # data_constituicao
                            'N/D'  # fax
                        )
                    all_data.append(item_data)
                    print(f"✅ Sociedade de Execução {item_data['Nome']} processada com sucesso!")
                except Exception as e:
                    logging.error(f"Erro ao processar sociedade de execução: {e}")
                    print(f"❌ Erro ao processar sociedade de execução: {e}")
            if all_data:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"dados_sociedades_execucao_{timestamp}.csv"
                save_to_csv(all_data, filename)
                print(f"✅ Dados salvos em CSV: {filename}")
            print(f"✅ Total de sociedades de execução coletadas: {len(all_data)}")
        return all_data
    except Exception as e:
        logging.error(f"Erro durante o scraping de sociedades: {str(e)}")
        print(f"❌ Erro durante o scraping de sociedades: {str(e)}")
        error_page = f"error_page_osae_sociedades_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
        with open(error_page, 'w', encoding='utf-8') as f:
            f.write(driver.page_source)
        print(f"📝 Página de erro salva em: {error_page}")
        return []


# Função para coletar dados de advogados de Angola
def coletar_advogados_angola(driver, conn):
    logging.info("Iniciando scraping de advogados de Angola...")
    print("🔎 Iniciando scraping de advogados de Angola...")
    base_url = "http://www.oaang.org/content/listagem-advogados-letra"
    all_data = []

    letras = ["a"] + [chr(i) for i in range(ord('b'), ord('z') + 1)]
    for idx, letra in enumerate(letras):
        if letra == "a":
            current_url = base_url
        else:
            current_url = f"{base_url}-{letra}"
        print(f"Acessando página: {current_url}")
        try:
            driver.get(current_url)
            WebDriverWait(driver, 30).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'table'))
            )
            if check_captcha(driver):
                driver.refresh()
                time.sleep(random.uniform(2, 4))

            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(random.uniform(2, 4))

            html = driver.page_source
            soup = BeautifulSoup(html, 'lxml')
            rows = soup.select('table tr')

            for row in rows[1:]:  # Pula o cabeçalho
                cols = row.select('td')
                if len(cols) >= 4:
                    nome = cols[0].get_text(strip=True)
                    cidade = cols[1].get_text(strip=True)
                    telefone = cols[2].get_text(strip=True)
                    email = cols[3].get_text(strip=True)

                    advogado_data = {
                        'nome': nome,
                        'cidade': cidade,
                        'telefone': telefone,
                        'email': email
                    }
                    all_data.append(advogado_data)

                    if conn:
                        inserir_advogado(
                            conn,
                            nome=nome,
                            cedula='N/D',
                            conselho_regional='OAANG',
                            morada=cidade,
                            estado_text='Ativo',
                            email=email,
                            telefone=telefone,
                            localidade=cidade
                        )

            print(f"✅ Dados coletados para letra {letra.upper()}")
            time.sleep(random.uniform(2, 4))

        except Exception as e:
            logging.error(f"Erro ao processar letra {letra.upper()}: {str(e)}")
            print(f"❌ Erro ao processar letra {letra.upper()}: {str(e)}")
            continue

    if all_data:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"dados_advogados_angola_{timestamp}.csv"
        save_to_csv(all_data, filename)
        print(f"✅ Dados salvos em CSV: {filename}")
    print(f"✅ Total de advogados coletados: {len(all_data)}")
    return all_data


# Função para coletar dados do Atlas CPLP
def coletar_atlas_cplp(driver, conn):
    logging.info("Iniciando scraping do Atlas CPLP...")
    print("🔎 Iniciando scraping do Atlas CPLP...")
    base_url = "https://www.atlascplp.csm.org.pt/"
    all_data = []

    driver.get(base_url)
    try:
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.ID, "navbar"))
        )
        if check_captcha(driver):
            driver.refresh()
            time.sleep(random.uniform(2, 4))

        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(random.uniform(2, 4))

        html = driver.page_source
        soup = BeautifulSoup(html, 'lxml')

        countries = soup.select("ol.flags a")
        for link in countries:
            try:
                country_name = link.get("title", "No title")
                href = link.get("href", "No href")
                flag_img = link.find("img")
                flag_src = flag_img.get("src") if flag_img else "No flag image"
                country_data = {
                    'Nome': country_name,
                    'Morada': href,
                    'Telefone': 'N/D',
                    'Email': 'N/D',
                    'Site': base_url,
                    'Tipo': 'atlas_cplp_country',
                    'Extra': f"Flag: {flag_src}"
                }
                if conn and conn.is_connected():
                    inserir_advogado(conn, country_name, 'N/D', 'N/D', href, 'N/D', 'N/D', base_url,
                                     'atlas_cplp_country', 'N/D', country_data['Telefone'])  # Pass Telefone here
                all_data.append(country_data)
                logging.info(f"País {country_name} processado com sucesso!")
                print(f"✅ País {country_name} processado com sucesso!")
            except Exception as e:
                logging.error(f"Erro ao processar país: {e}")
                print(f"Erro ao processar país: {e}")

        topics = soup.select("a.home-subjects")
        for topic in topics:
            try:
                title = topic.get("title", "No title")
                data_id = topic.get("data-id", "No data-id")
                icon = topic.find("i")
                icon_class = icon.get("class", ["No icon"])[-1] if icon else "No icon"
                topic_data = {
                    'Nome': title,
                    'Morada': 'N/D',
                    'Telefone': 'N/D',
                    'Email': 'N/D',
                    'Site': base_url,
                    'Tipo': 'atlas_cplp_topic',
                    'Extra': f"Data-ID: {data_id}, Icon: {icon_class}"
                }
                if conn and conn.is_connected():
                    inserir_advogado(conn, title, 'N/D', 'N/D', 'N/D', 'N/D', 'N/D', base_url,
                                     'atlas_cplp_topic', 'N/D', topic_data['Telefone'])  # Pass Telefone here
                all_data.append(topic_data)
                logging.info(f"Tema judicial {title} processado com sucesso!")
                print(f"✅ Tema judicial {title} processado com sucesso!")
            except Exception as e:
                logging.error(f"Erro ao processar tema judicial: {e}")
                print(f"Erro ao processar tema judicial: {e}")

        internal_links = soup.select("#home-page-interns a")
        for link in internal_links:
            try:
                href = link.get("href", "No href")
                text = link.find("span") or link.find("h4")
                link_text = text.text.strip() if text else "No text"
                internal_link_data = {
                    'Nome': link_text,
                    'Morada': href,
                    'Telefone': 'N/D',
                    'Email': 'N/D',
                    'Site': base_url,
                    'Tipo': 'atlas_cplp_internal_link'
                }
                if conn and conn.is_connected():
                    inserir_advogado(conn, link_text, 'N/D', 'N/D', href, 'N/D', 'N/D', base_url,
                                     'atlas_cplp_internal_link', 'N/D',
                                     internal_link_data['Telefone'])  # Pass Telefone here
                all_data.append(internal_link_data)
                logging.info(f"Link interno {link_text} processado com sucesso!")
                print(f"✅ Link interno {link_text} processado com sucesso!")
            except Exception as e:
                logging.error(f"Erro ao processar link interno: {e}")
                print(f"Erro ao processar link interno: {e}")

        footer = soup.find("footer")
        footer_links = footer.find_all("a") if footer else []
        for link in footer_links:
            try:
                href = link.get("href", "No href")
                img = link.find("img")
                alt_text = img.get("alt", "No alt text") if img else "No alt text"
                footer_link_data = {
                    'Nome': alt_text,
                    'Morada': href,
                    'Telefone': 'N/D',
                    'Email': 'N/D',
                    'Site': base_url,
                    'Tipo': 'atlas_cplp_footer_link'
                }
                if conn and conn.is_connected():
                    inserir_advogado(conn, alt_text, 'N/D', 'N/D', href, 'N/D', 'N/D', base_url,
                                     'atlas_cplp_footer_link', 'N/D',
                                     footer_link_data['Telefone'])  # Pass Telefone here
                all_data.append(footer_link_data)
                logging.info(f"Link do footer {alt_text} processado com sucesso!")
                print(f"✅ Link do footer {alt_text} processado com sucesso!")
            except Exception as e:
                logging.error(f"Erro ao processar link do footer: {e}")
                print(f"Erro ao processar link do footer: {e}")

    except Exception as e:
        logging.error(f"Erro ao carregar dados do Atlas CPLP: {e}")
        print(f"Erro ao carregar dados do Atlas CPLP: {e}")
        with open(f"error_page_atlas_cplp.html", 'w', encoding='utf-8') as f:
            f.write(driver.page_source)

    if all_data:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        save_to_csv(all_data, f"atlas_cplp_{timestamp}.csv")

    logging.info(f"Total de itens do Atlas CPLP coletados: {len(all_data)}")
    print(f"✅ Total de itens do Atlas CPLP coletados: {len(all_data)}")
    return all_data


def inserir_tribunal(conexao, nome, morada, telefone, email):
    try:
        cursor = conexao.cursor()
        query = """
            INSERT INTO tribunais (nome, morada, telefone, email)
            VALUES (%s, %s, %s, %s)
        """
        dados = (
            str(nome or 'N/D'),
            str(morada or 'N/D'),
            str(telefone or 'N/D'),
            str(email or 'N/D')
        )
        cursor.execute(query, dados)
        conexao.commit()
        cursor.close()
        print(f"✅ Tribunal {nome} inserido na tabela tribunais!")
    except Exception as e:
        print(f"❌ Erro ao inserir tribunal {nome}: {e}")
        conexao.rollback()


def marcar_todos_checkboxes(driver, timeout=20):
    # Descobre quantos checkboxes existem inicialmente
    checkboxes = driver.find_elements(By.CSS_SELECTOR, 'input[type="checkbox"][id*="chkCategory"]')
    total = len(checkboxes)
    print(f"Encontrados {total} checkboxes de região.")

    for i in range(total):
        # Sempre recarrega a lista de checkboxes após cada clique/postback
        checkboxes = driver.find_elements(By.CSS_SELECTOR, 'input[type="checkbox"][id*="chkCategory"]')
        checkbox = checkboxes[i]
        if not checkbox.is_selected():
            driver.execute_script("arguments[0].click();", checkbox)
            # Aguarda o checkbox ficar selecionado novamente (espera o postback)
            WebDriverWait(driver, timeout).until(
                lambda d: d.find_elements(By.CSS_SELECTOR, 'input[type="checkbox"][id*="chkCategory"]')[i].is_selected()
            )
            print(f"Checkbox {i + 1} marcado. Aguardando 10 segundos para o recarregamento...")
            time.sleep(10)
        else:
            print(f"Checkbox {i + 1} já estava marcado.")


# Função principal
def main():
    logging.info("Iniciando o script de scraping...")
    print(f"🏁 Iniciando o script de scraping às {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}...")
    conn = conectar_mysql()
    todos_dados = []

    funcoes_scraping = [
         (coletar_advogados_lisboa, False),
           (coletar_advogados_porto, False),
           (coletar_advogados_coimbra, False),
         (coletar_advogados_evora, False),
          (coletar_advogados_faro, False),
         (coletar_advogados_madeira, False),
         (coletar_advogados_acores, False),
         (coletar_advogados_braga, False),
         (coletar_sociedades_lisboa, False),
         (coletar_sociedades_porto, False),
         (coletar_sociedades_coimbra, False),
         (coletar_sociedades_evora, False),
         (coletar_sociedades_faro, False),
         (coletar_sociedades_madeira, False),
         (coletar_sociedades_acores, False),
         (coletar_sociedades_braga, False),
         (coletar_estagiarios_lisboa, False),
           (coletar_estagiarios_porto, False),
          (coletar_estagiarios_coimbra, False),
          (coletar_estagiarios_evora, False),
         (coletar_estagiarios_faro, False),
          (coletar_estagiarios_madeira, False),
          (coletar_estagiarios_acores, False),
          (coletar_estagiarios_braga, False),
         (coletar_tribunais, True),
        (coletar_julgados, True),
         (scrape_osae, True),
         (scrape_osae_sociedades, True),
        (coletar_advogados_angola, True),
         (coletar_atlas_cplp, True)
    ]

    driver = configurar_driver()
    if not driver:
        print("Falha ao iniciar o WebDriver. Encerrando...")
        return

    try:
        for func, single_page in funcoes_scraping:
            try:
                logging.info(f"Iniciando: {func.__name__}")
                print(f"\n{'=' * 50}")
                print(f"🏁 Iniciando: {func.__name__}")
                dados = func(driver, conn)

                if dados:
                    logging.info(f"{func.__name__} completado com {len(dados)} registros")
                    print(f"✅ {func.__name__} completado com {len(dados)} registros")
                    todos_dados.extend(dados)
                else:
                    logging.warning(f"{func.__name__} retornou 0 registros")
                    print(f"⚠️ {func.__name__} retornou 0 registros")

            except Exception as e:
                logging.error(f"Erro em {func.__name__}: {str(e)}")
                print(f"❌ Erro em {func.__name__}: {str(e)}")
                continue

        if todos_dados:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            save_to_csv(todos_dados, f"todos_dados_{timestamp}.csv")
            logging.info(f"Todos os dados salvos em CSV com timestamp: {timestamp}")
            print(f"✅ Todos os dados salvos em CSV com timestamp: {timestamp}")

    finally:
        if driver:
            driver.quit()
        if conn and conn.is_connected():
            conn.close()
            logging.info("Conexão com o banco de dados fechada.")
            print("Conexão com o banco de dados fechada.")


if __name__ == "__main__":
    main()