import httpx
import arrow
import orjson
import pathlib
minLat = -36
maxLat = -34
minLon = 148
maxLon = 150


def archive_luftdaten(event, context):
    ddb = boto3.client('dynamodb', endpoint_url='http://localhost:8000')
    metadaten = ddb.Table('metadatenTable')
    last_modified = metadaten.get_item(Key={'metadataKey': 'last_modified'}).get(
        'Item', {}).get('metadataValue', "Sat, 18 Jan 2020 08:22:25 GMT")
    r = httpx.get("https://data.sensor.community/static/v2/data.dust.min.json",
                  headers={'If-Modified-Since': last_modified})
    if r.status_code != 200:
        print("No action for http status %d" % r.status_code)
        return
    last_modified['metadataValue'] = r.headers['Last-Modified']
    metadaten.put_item(last_modified)
    luftdaten = orjson.loads(r.text())
    #luftdaten = orjson.loads(pathlib.Path('data.dust.min.json').read_text())
    canberra_daten = []
    i = 0
    with ddb.Table('luftdatenTable').batch_writer() as batch:
        for daten in luftdaten:
            i += 1
            if i % 1000 == 0:
                print(i)
            if i > 10000:
                lat = float(daten['location']['latitude']
                            ) if daten['location']['latitude'] != '' else 0
                lon = float(daten['location']['longitude']
                            ) if daten['location']['longitude'] != '' else 0
                if minLat < lat < maxLat and minLon < lon < maxLon:
                    updateTime = arrow.get(daten['timestamp'])
                    PM10 = None
                    PM2_5 = None
                    for value in daten['sensordatavalues']:
                        if value['value_type'] == "P1":
                            PM10 = float(value['value'])
                        if value['value_type'] == "P2":
                            PM2_5 = float(value['value'])
                    datum = {"updateTime": int(updateTime.format('X')),
                             "expiryTime": updateTime.shift(hours=+24).format('X'), "recordId": daten['id'],
                             "locationId": daten['location']['id'], "lat": lat, "lon": lon,
                             "PM10": PM10, "PM2_5": PM2_5}
                    # print(daten)
                    print(datum)
                    canberra_daten.append(datum)
                    batch.put_item(Item=datum)
    body = {
        "message": "Archived!",
        "input": event
    }

    response = {
        "statusCode": 200,
        "body": orjson.dumps(body)
    }

    return response


if __name__ == "__main__":
    archive_luftdaten('', '')
