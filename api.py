import boto3
from boto3.dynamodb.conditions import Key, Attr
import json
import os

#TODO 24 hour history using table scan, grouped by sensor
# https://www.earthdatascience.org/courses/use-data-open-source-python/use-time-series-data-in-python/resample-time-series-data-pandas-python/
# https://stackoverflow.com/questions/45156289/pandas-where-is-the-documentation-for-timegrouper
def get_daily_history(event, context):
    ddb = boto3.resource('dynamodb',
     endpoint_url=(None if os.environ.get('SERVERLESS_STAGE',None) == 'prod' else 'http://localhost:8000'))

    response = ddb.Table('luftdatenTable').query(
        KeyConditionExpression=Key('luftdaten').eq(1) & Key('updateTime').gt(1) 
    )
    result = {}
    for item in response['Items']:
        locationId = int(item['locationId'])
        if locationId not in result:
            result[locationId] = {'lat':float(item['lat']), 'lon':float(item['lon']), 'PM10': {}, 'PM2_5': {}}
        result[locationId]['PM10'][int(item['updateTime'])] = float(item['PM10'])
        result[locationId]['PM2_5'][int(item['updateTime'])] = float(item['PM2_5'])
    response = {
        "statusCode": 200,
        "body": json.dumps(result)
    }

    return response

# TODO 10m, 30m averages
def get_latest(event, context):

    ddb = boto3.resource('dynamodb',
     endpoint_url=(None if os.environ.get('SERVERLESS_STAGE',None) == 'prod' else 'http://localhost:8000'))

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
      ':val': 1579433200 now - 45 minutes
    },

};
docClient.query(params, function(err, data) {
    if (err) ppJson(err); // an error occurred
    else ppJson(data); // successful response
});

for item in items
if time > now-30 minutes
if time > now-10 minutes
if last item for that sensor, is latest

'''
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
    print(get_daily_history('', ''))
    print(get_latest('', ''))
