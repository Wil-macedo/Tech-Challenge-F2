
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from flask import request
from selenium import webdriver
import pandas as pd
from libs.moveS3 import moveToS3
import os
import time
import re
import os


tempPath = os.path.join(os.path.abspath(os.curdir),"temp_files") + (os.path.sep)  # Temp dir

chrome_options = Options()
chrome_options.add_experimental_option('prefs', {
    "download.default_directory": tempPath,  # Mude para o diretório desejado
    "download.prompt_for_download": False,
})

# Adiciona a opção para rodar o navegador em modo headless
chrome_options.add_argument("--headless")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")

# Baixa, configura automaticamente o ChromeDriver
service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=chrome_options)  # Inicialização


def getcsv():
    try:
        date = request.args.get('data')
        
        if date is None or not re.match(r"^(\d{4})-(\d{2})-(\d{2})$", date):
            # TODO: Se não tiver data, podemos pegar o ultimo dia útil talvez :/
            date = "2024-08-31"
        
        urlGet = f"https://arquivos.b3.com.br/tabelas/TradeInformationConsolidatedAfterHours/{date}??lang=pt"
        driver.get(urlGet)
        
        # Espera até que o botão de download esteja presente e clicável, e clica nele
        download_button = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.XPATH, '/html/body/div/div/div/div/div[2]/div[3]/div[1]/div[2]/p/a'))
        )
        download_button.click()
        
        # Espera até que o arquivo .crdownload seja criado e então renomeia o arquivo
        temp_files = [f for f in os.listdir(tempPath) if f.endswith('.crdownload')]
        
        # Aguarda o arquivo ser completamente baixado
        while temp_files:
            time.sleep(.5)
            temp_files = [f for f in os.listdir(tempPath) if f.endswith('.crdownload')]
        
        time.sleep(5)  # Delay de segurança para garantir que arquivo foi baixado.
        
        for file in os.listdir(tempPath):
            source_file = os.path.join(tempPath, file)
            match = re.search(r'_(\d{8})_', file)

            if match:
                date = match.group(1)
                fileName = date[:4] + "-" + date[4:6] + "-" + date[6:8] + ".parquet"  # 2024-08-30
                
                fullPath = os.path.join(tempPath,  fileName) 
        
                # Arquivo é salvo em diretório temporário > convetido > movido.
                df = pd.read_csv(source_file, delimiter=';')

                # Salva o DataFrame como um arquivo Parquet
                df.to_parquet(fullPath, engine='pyarrow', index=True)
                
                moveToS3(fullPath, fileName)
                os.remove(source_file)
                
                result = f"Arquivo baixado com sucesso, convertido em parquet & movido - {fullPath}"
                
            else:
                os.remove(source_file)
                result = "Não há data no arquivo baixado, **DELETED**"
                
    except:
        result = f"ERRO - Não há movimentações para esta data"
    
    return result