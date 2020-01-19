import boto3
import json


def hello(event, context):

    # For a Boto3 client.
    ddb = boto3.client('dynamodb', endpoint_url='http://localhost:8000')
    response = ddb.list_tables()
    print(response)

    # For a Boto3 service resource
    ddb = boto3.resource('dynamodb', endpoint_url='http://localhost:8000')
    print(list(ddb.tables.all()))

    body = {
        "message": "Go Serverless v1.0! Your function executed successfully!",
        "input": event
    }

    response = {
        "statusCode": 200,
        "body": json.dumps(body)
    }

    return response

    # Use this code if you don't use the http event with the LAMBDA-PROXY
    # integration
    """
    return {
        "message": "Go Serverless v1.0! Your function executed successfully!",
        "event": event
    }
    """
