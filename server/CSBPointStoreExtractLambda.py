import json
import time
import boto3

# athena constant
DATABASE = 'csbathenadb'
TABLE = 'csb_view'
COLUMN = 'NAME'
S3_OUTPUT = 's3://aws-athena-query-results-411735806573-us-east-2/'
S3_ACCESS = 'https://s3.us-east-2.amazonaws.com/aws-athena-query-results-411735806573-us-east-2/'
RETRY_COUNT = 3


def lambda_handler(event, context):
    # TODO implement
    uuid = str(event['uuid'])
    email = str(event['email'])
    name = str(event['platform.name'])
    bbox = str(event['bbox'])

    # created query
    query = "SELECT * FROM %s.%s where %s = '%s' limit 1000;" % (DATABASE, TABLE, COLUMN, name)

    # TODO Support spatial query
    # e.g. SELECT COUNT(*) cnt
    # FROM csbathenadb.csb_view
    # WHERE ST_INTERSECTS (ST_POLYGON('polygon((-140.0 24.0, -110.0 24.0, -110.0 32.0, -140.0 32.0))'), ST_POINT(csb_view.lon, csb_view.lat))

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
    print("QEId:" + query_execution_id + "----------")

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
    # result = client.get_query_results(QueryExecutionId=query_execution_id)

    access_url = S3_ACCESS + query_execution_id + ".csv"
    print("URL:" + access_url)

    return {
        'statusCode': 200,
        'body': json.dumps("Order is processing. You will receive an email when your file is ready")
    }