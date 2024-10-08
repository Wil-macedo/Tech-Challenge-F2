
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from libs.moveS3 import moveToS3
from selenium import webdriver
from flask import request
from .sumDay import sumDay
import pandas as pd
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
    result = ""
    dateParam = request.args.get('data')
    offset = request.args.get('offset', 1, type=int)
    
    
    for i in range(0, offset):  # Pega próximos dias do offset.
        try:
            if dateParam is None or not re.match(r"^(\d{4})-(\d{2})-(\d{2})$", dateParam):
                # TODO: Se não tiver data, podemos pegar o ultimo dia útil talvez :/
                dateParam = "2024-08-31"
            
            urlGet = f"https://arquivos.b3.com.br/tabelas/TradeInformationConsolidatedAfterHours/{dateParam}"
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
                    df = pd.read_csv(source_file, delimiter=';', skiprows=1, header=0)

                    # Salva o DataFrame como um arquivo Parquet
                    df.to_parquet(fullPath, engine='pyarrow', index=True)
                    
                    moveToS3(fullPath, "rawData/" + fileName)
                    os.remove(source_file)
                    
                    result += f"OK - DATA: {dateParam} - Arquivo baixado com sucesso, convertido em parquet & movido - {fullPath} <br><br>"
                    
                else:
                    os.remove(source_file)
                    result += f"{dateParam} - Não há informações para essa data, **DELETED** <br><br>"
                    
        except Exception as ex:    
            result += f"ERRO - DATA: {dateParam} - {str(ex)} <br><br>"
        
        finally:
            dateParam = sumDay(dateParam)
            
            
    return result