import boto3
from boto3.dynamodb.conditions import Key, Attr
import json


def get_latest(event, context):

    ddb = boto3.resource('dynamodb', endpoint_url='http://localhost:8000')

    body = {
        "message": "Go Serverless v1.0! Your function executed successfully!",
        "input": event,
        "tables": list(ddb.tables.all())
    }
# response = table.query(
#     KeyConditionExpression=Key('username').eq('johndoe')
# )
# items = response['Items']
# print(items)
    response = {
        "statusCode": 200,
        "body": json.dumps(body)
    }

    return response
