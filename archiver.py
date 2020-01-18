import httpx
import arrow

etag = "44b5e3-59c6524c989b4"
last_modified = "Sat, 18 Jan 2020 07:38:26 GMT"

#r = httpx.get("https://api.luftdaten.info/static/v2/data.dust.min.json", headers={'If-None-Match': etag})
r = httpx.head("https://api.luftdaten.info/static/v2/data.dust.min.json", headers={'If-None-Match': etag})
if r.status_code == 200:
    print (r,r.headers)
    etag = r.headers['Etag']
    last_modified_date = arrow.get(r.headers['Last-Modified'], "ddd, DD MMM YYYY HH:mm:ss ZZZ")
    print("Etag: %s, Last Modified: %s" % (etag, last_modified_date.format()))
else:
    print ("No action for http status %d" % r.status_code)
