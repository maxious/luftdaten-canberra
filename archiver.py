import httpx
import arrow
RFC822_DATE= "ddd, DD MMM YYYY HH:mm:ss ZZZ"

last_modified= "Sat, 18 Jan 2020 08:22:25 GMT"

#r = httpx.get("https://api.luftdaten.info/static/v2/data.dust.min.json", headers={'If-None-Match': etag})
#r = httpx.head("https://api.luftdaten.info/static/v2/data.dust.min.json", headers={'If-None-Match': etag})
r = httpx.head("https://api.luftdaten.info/static/v2/data.dust.min.json", headers={'If-Modified-Since': last_modified})
if r.status_code == 200:
    print (r,r.headers)
    last_modified_date = arrow.get(r.headers['Last-Modified'],RFC822_DATE) 
    print("Last Modified: %s" %  last_modified_date.format('DDMMYYY-HHmmss.X'))
    print("")
    print('last_modified= "%s"' % r.headers['Last-Modified'])
else:
    print ("No action for http status %d" % r.status_code)
