import httpx
import arrow
import orjson
import pathlib
RFC822_DATE = "ddd, DD MMM YYYY HH:mm:ss ZZZ"
minLat = -36
maxLat = -34
minLon = 148
maxLon = 150


def archive_luftdaten(event, context):
    # last_modified= "Sat, 18 Jan 2020 08:22:25 GMT"

    # r = httpx.head("https://data.sensor.community/static/v2/data.dust.min.json", headers={'If-Modified-Since': last_modified})
    # if r.status_code == 200:
    #     print (r,r.headers)
    #     last_modified_date = arrow.get(r.headers['Last-Modified'],RFC822_DATE)
    #     print("Last Modified: %s" %  last_modified_date.format('DDMMYYY-HHmmss.X'))
    #     print("")
    #     print('last_modified= "%s"' % r.headers['Last-Modified'])
    # else:
    #     print ("No action for http status %d" % r.status_code)

    luftdaten = orjson.loads(pathlib.Path('data.dust.min.json').read_text())
    canberra_daten = []
    i = 0
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
