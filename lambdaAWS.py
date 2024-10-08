import json
import boto3
from datetime import datetime


def lambda_handler(event, context):

    gluJobName = 'ETL-TC'
    
    glue_client = boto3.client('glue')

    try:
        # Inicia o job de ETL
        response = glue_client.start_job_run(JobName=gluJobName)
        now = datetime.now().strftime('%d-%m-%Y')
        
        return {
            'statusCode': 200,
            'body': json.dumps(f'Job iniciado com sucesso - {now}: {response["JobRunId"]}')
        }
    
    except Exception as ex:
        return {
            'statusCode': 500,
            'body': json.dumps(f'Erro ao iniciar o job: {str(ex)}')
        }
