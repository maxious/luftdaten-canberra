import boto3
from boto3.dynamodb.conditions import Key, Attr
import json


def get_latest(event, context):

    ddb = boto3.resource('dynamodb') #, endpoint_url='http://localhost:8000')

    body = {
        "message": "Go Serverless v1.0! Your function executed successfully!",
        "input": event,
        "tables": [str(x) for x in ddb.tables.all()]
    }
    '''
   var params = {
    TableName: 'luftdatenTable',
    KeyConditionExpression: 'luftdaten = :luftdaten AND updateTime > :val',
    ExpressionAttributeValues: { // a map of substitutions for all attribute values
   ':luftdaten' : 1,
      ':val': 1579433200
    },

};
docClient.query(params, function(err, data) {
    if (err) ppJson(err); // an error occurred
    else ppJson(data); // successful response
});'''
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


if __name__ == "__main__":
    print(get_latest('', ''))
