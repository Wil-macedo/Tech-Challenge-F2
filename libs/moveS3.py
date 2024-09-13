import boto3
import os

# AS CREDENCIAIS NÃO ESTÃO SENDO UTILIZADAS, POIS JÁ FORAM
#CONFIGURADAS UTILIZANDO O AWS CLI.

#AWS_ACESS_KET_ID = os.environ.get('AWS_ACESS_KET_ID')
#AWS_SECRET_ACESS_KEY = os.environ.get('AWS_SECRET_ACESS_KEY')
AWS_BUCKET_NAME = os.environ.get('AWS_BUCKET_NAME')

def moveToS3(fullPath:str, fileName:str):

    client = boto3.client(
        's3',
        region_name="us-east-1",
    )

    try:
        client.upload_file(fullPath, AWS_BUCKET_NAME, fileName)
        os.remove(fullPath)  # Remove parquet do app service.
        
    except Exception as ex:
        print("FALHA AO FAZER UPLOAD")    
    client.close()