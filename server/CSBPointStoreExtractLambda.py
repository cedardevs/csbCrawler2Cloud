import json
import time
import boto3

# athena constant
DATABASE = 'csbathenadb'
TABLE = 'csb_view'
COLUMN = 'UUID'
S3_OUTPUT = 's3://aws-athena-query-results-411735806573-us-east-2/'
RETRY_COUNT = 3


def lambda_handler(event, context):

    uuid = str(event['uuid'])
    email = str(event['email'])
    message = 'Query for ' + uuid

    # created query
    # query = "SELECT count(*) mycnt FROM %s.%s where %s = '%s';" % (DATABASE, TABLE, COLUMN, uuid)
    query = 'SELECT * FROM "csbathenadb"."xyz" limit 10;'
    print(query)

    # athena client
    client = boto3.client('athena')

    # Execution
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': DATABASE
        },
        ResultConfiguration={
            'OutputLocation': S3_OUTPUT,
        }
    )

    # get query execution id
    query_execution_id = response['QueryExecutionId']
    print(query_execution_id)

    # get execution status
    for i in range(1, 1 + RETRY_COUNT):

        # get query execution
        query_status = client.get_query_execution(QueryExecutionId=query_execution_id)
        query_execution_status = query_status['QueryExecution']['Status']['State']

        if query_execution_status == 'SUCCEEDED':
            print("STATUS:" + query_execution_status)
            break

        if query_execution_status == 'FAILED':
            # raise Exception("STATUS:" + str(query_execution_status))
            print("FAILED")
            return None

        else:
            print("STATUS:" + query_execution_status)
            time.sleep(10)
    else:
        client.stop_query_execution(QueryExecutionId=query_execution_id)
        raise Exception('TIME OVER')

    # get query results
    result = client.get_query_results(QueryExecutionId=query_execution_id)
    print(result)

    # get data
    if len(result['ResultSet']['Rows']) == 2:

        email = result['ResultSet']['Rows'][1]['Data'][1]['VarCharValue']

        return email

    else:
        return None

    return {
        'statusCode': 200,
        'body': json.dumps(query)
    }
