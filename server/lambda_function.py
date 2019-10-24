import json
import time
import boto3

# athena constant
DATABASE = 'csbathenadb'
TABLE = 'csb_mv'
S3_OUTPUT = 's3://nesdis-ncei-csb/output/'
S3_ACCESS = 'https://s3.us-east-2.amazonaws.com/nesdis-ncei-csb/output/'
RETRY_COUNT = 7
SLEEP = 2


def lambda_handler(event, context):
    recipient = event.get('email')
    bbox_polygon = None
    platform_name = event.get('platform.name')
    bbox = event.get('bbox')
    if bbox:
        bbox_coords = bbox.split(",")
        bbox_polygon = bbox_coords[0] + " " + bbox_coords[1] + ", " + bbox_coords[2] + " " + bbox_coords[1] + ", " + \
                       bbox_coords[2] + " " + bbox_coords[3] + ", " + bbox_coords[0] + " " + bbox_coords[3]
    sdate = event.get('sdate')
    edate = event.get('edate')

    where_clause = None
    # create query
    if platform_name:
        where_clause = " where NAME = '" + platform_name + "'"

    if where_clause and bbox:
        where_clause = where_clause + " and ST_INTERSECTS (ST_POLYGON('polygon((" + bbox_polygon + "))'), ST_POINT(" + TABLE + ".lon, " + TABLE + ".lat))"
    elif bbox:
        where_clause = " where ST_INTERSECTS (ST_POLYGON('polygon((" + bbox_polygon + "))'), ST_POINT(" + TABLE + ".lon, " + TABLE + ".lat))"

    if where_clause and sdate and edate:
        where_clause = where_clause + " and from_iso8601_timestamp(" + TABLE + ".time) BETWEEN from_iso8601_timestamp('" + sdate + "') AND from_iso8601_timestamp('" + edate + "')"
    elif sdate and edate:
        where_clause = " where from_iso8601_timestamp(" + TABLE + ".time) BETWEEN from_iso8601_timestamp('" + sdate + "') AND from_iso8601_timestamp('" + edate + "')"

    if where_clause:
        query = "SELECT * FROM %s.%s %s limit 10000;" % (DATABASE, TABLE, where_clause)
    else:
        # Return error need constraint
        return {
            'statusCode': 400,
            'body': json.dumps(
                "ERROR: Request must include platform_name, sdate and edate, or bounding box constraints")
        }

    print("query=" + query)

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
            print("STATUS:" + str(query_execution_status))
            print("FAILED")
            return None

        else:
            print("STATUS:" + query_execution_status)
            time.sleep(SLEEP)
    else:
        client.stop_query_execution(QueryExecutionId=query_execution_id)
        raise Exception('TIME OVER')

    access_url = S3_ACCESS + query_execution_id + ".csv"
    print("URL:" + access_url)

    return {
        'statusCode': 200,
        'access_url': access_url
    }

