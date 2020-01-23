import boto3
from boto3.dynamodb.conditions import Key, Attr
import json
import os
import arrow
import pandas as pd

PROD = os.environ.get('SERVERLESS_STAGE', None) == 'prod'

# TODO 24 hour history using table scan, grouped by sensor
# https://www.earthdatascience.org/courses/use-data-open-source-python/use-time-series-data-in-python/resample-time-series-data-pandas-python/
# https://stackoverflow.com/questions/45156289/pandas-where-is-the-documentation-for-timegrouper

def get_daily_history(event, context):
    ddb = boto3.resource('dynamodb', endpoint_url=(None if PROD else 'http://localhost:8000'))
    day_ago = int(arrow.now().shift(hours=-24).format('X'))
    response = ddb.Table('luftdatenTable').query(
        KeyConditionExpression=Key('luftdaten').eq(1)
        & Key('updateTime').gt(day_ago if PROD else 1)
    )
    result = {}
    for item in response['Items']:
        locationId = int(item['locationId'])
        if locationId not in result:
            result[locationId] = {'lat': float(item['lat']), 'lon': float(
                item['lon']), 'PM10': {}, 'PM2_5': {}}
        result[locationId]['PM10'][int(
            item['updateTime'])] = float(item['PM10'])
        result[locationId]['PM2_5'][int(
            item['updateTime'])] = float(item['PM2_5'])
    response = {
        "statusCode": 200,
        "body": json.dumps(result)
    }

    return response


def _items_to_location_dataframe(items):
    df = pd.DataFrame.from_records(items, exclude=['recordId','luftdaten','expiryTime'])
    locations = (df.groupby('locationId')[['lat','lon']].max().to_dict(orient="index"))
    df = df.drop('lat', axis=1).drop('lon', axis=1)
    df['datetime'] = pd.to_datetime(df['updateTime'].astype('int'), unit='s')
    df = df.set_index('datetime')
    df.drop(['updateTime'], axis=1, inplace=True)
    #df.to_csv('records.csv')
    return locations, df
def get_latest(event, context):

    ddb = boto3.resource('dynamodb', endpoint_url=(None if PROD else 'http://localhost:8000'))
    now = arrow.now() if PROD else arrow.get(1579695768)
    three_quarter_hour_ago = int(now.shift(minutes=-45).format('X'))
    # half_hour_ago = int(now.shift(minutes=-30).format('X'))
    # quarter_hour_ago = int(now.shift(minutes=-15).format('X'))
    response = ddb.Table('luftdatenTable').query(
        KeyConditionExpression=Key('luftdaten').eq(1)
        & Key('updateTime').gt(three_quarter_hour_ago if PROD else 1)
    )
    result = {}
    data = {}
    locations, df = _items_to_location_dataframe(response['Items'])
    # for item in response['Items']:
    #     locationId = int(item['locationId'])
    #     if locationId not in result:
    #         result[locationId] = {'lat': float(
    #             item['lat']), 'lon': float(item['lon'])}
    #     if locationId not in data:
    #         data[locationId] = {
    #             '30min_PM10': [], '30min_PM2_5': [], 
    #                '10min_PM10': [], '10min_PM2_5': [], 
    #         'latest_time': 0}
    #     if int(item['updateTime']) > half_hour_ago:
    #         data[locationId]['current_PM10'][int(
    #             item['updateTime'])] = float(item['PM10'])
    #         result[locationId]['current_PM2_5'][int(
    #             item['updateTime'])] = float(item['PM2_5'])
    #         data[locationId]['latest_time'] = int(item['updateTime'])
    #     if int(item['updateTime']) > data[locationId].get('latest_time', 0):
    #         result[locationId]['current_PM10'][int(
    #             item['updateTime'])] = float(item['PM10'])
    #         result[locationId]['current_PM2_5'][int(
    #             item['updateTime'])] = float(item['PM2_5'])
    #         data[locationId]['latest_time'] = int(item['updateTime'])
    #     if int(item['updateTime']) > data[locationId].get('latest_time', 0):
    #         result[locationId]['current_PM10'][int(
    #             item['updateTime'])] = float(item['PM10'])
    #         result[locationId]['current_PM2_5'][int(
    #             item['updateTime'])] = float(item['PM2_5'])
    #         data[locationId]['latest_time'] = int(item['updateTime'])

    # for locationId, datum in data.items():
    #     np.mean([c.a for c in c_list])
    response = {
        "statusCode": 200,
        "body": json.dumps(result)
    }

    return response


if __name__ == "__main__":
    #print(get_daily_history('', ''))
    print(get_latest('', ''))
