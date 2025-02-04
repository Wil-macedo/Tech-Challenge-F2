# getcsv - Download e Conversão de Arquivos da B3

## Descrição

O endpoint `/getData` faz o download de arquivos CSV da B3, converte para o formato Parquet e armazena no S3. O processo inclui:

1. Coleta da data fornecida como parâmetro ou uso de uma data padrão.
2. Acessa o site da B3 para baixar o arquivo correspondente.
3. Aguarda a conclusão do download e renomeia o arquivo conforme a data extraída.
4. Converte o CSV em Parquet.
5. Move o arquivo final para o S3.
6. Retorna o status da operação.

---

## Parâmetros da Requisição

### Query Parameters

| Parâmetro | Tipo     | Descrição                                                                    |
| --------- | -------- | ---------------------------------------------------------------------------- |
| `data`    | `string` | (Opcional) Data no formato `YYYY-MM-DD`. Se não informado, usa `2024-08-31`. |
| `offset`  | `int`    | (Opcional) Número de dias consecutivos para processar. Padrão: `1`.          |

---

## Fluxo de Execução

1. **Validação da Data:** Verifica se a data fornecida está no formato correto (`YYYY-MM-DD`). Se não estiver, utiliza `2024-08-31`.

2. **Geração da URL:** Monta a URL do arquivo CSV no site da B3.

3. **Download Automático:** Utiliza Selenium para acessar o site e clicar no botão de download.

4. **Aguardando Download:** O script verifica quando o arquivo `.crdownload` desaparece, garantindo que o download foi concluído.

5. **Processamento do Arquivo:**
   - Renomeia o arquivo conforme a data extraída.
   - Carrega o CSV usando `pandas` e converte para Parquet.
   - Move o arquivo convertido para um bucket S3.
   - Remove o arquivo temporário após a transferência.

6. **Execução para Múltiplas Datas:** Caso o `offset` seja maior que 1, repete o processo para os dias subsequentes.

7. **Resposta HTTP:** Retorna mensagens de sucesso ou erro conforme o andamento do processo.

### Exemplo de chamada:

```sh
GET /getData?data=2024-08-30&offset=3
```

### Resposta esperada:

```
OK - DATA: 2024-08-30 - Arquivo baixado com sucesso, convertido em parquet & movido - /tmp/2024-08-30.parquet
OK - DATA: 2024-08-31 - Arquivo baixado com sucesso, convertido em parquet & movido - /tmp/2024-08-31.parquet
2024-09-01 - Não há informações para essa data, **DELETED**
```

Caso ocorra um erro, a mensagem será retornada com detalhes.

Necessário adicionar as respectivas credenciais do AWS S3.
***AWS_ACESS_KET_ID***
***AWS_SECRET_ACESS_KEY***
***AWS_BUCKET_NAME***