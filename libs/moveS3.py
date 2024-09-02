import boto3
import os


AWS_ACESS_KET_ID = os.environ.get('AWS_ACESS_KET_ID')
AWS_SECRET_ACESS_KEY = os.environ.get('AWS_SECRET_ACESS_KEY')
AWS_BUCKET_NAME = os.environ.get('AWS_BUCKET_NAME')

def moveToS3(fullPath:str, fileName:str):

    client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACESS_KET_ID,
        aws_secret_access_key=AWS_SECRET_ACESS_KEY
    )

    try:
        client.upload_file(fullPath, AWS_BUCKET_NAME, fileName)
        os.remove(fullPath)  # Remove parquet do app service.
        
    except Exception as ex:
        print("FALHA AO FAZER UPLOAD")    
    client.close()