import json
import boto3

def lambda_handler(event, context):
    # Nome do job do Glue
    gluJobName = 'ETL_V2'
    
    
    glue_client = boto3.client('glue')

    try:
        # Inicia o job de ETL
        response = glue_client.start_job_run(JobName=gluJobName)
        
        return {
            'statusCode': 200,
            'body': json.dumps(f'Job iniciado com sucesso: {response["JobRunId"]}')
        }
    
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f'Erro ao iniciar o job: {str(e)}')
        }
