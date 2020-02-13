# luftdaten-canberra
Prototype of AWS lambda caching for luftdaten data for canberra

## development
https://serverless.com/plugins/serverless-dynamodb-local/

``` 
npm install -g serverless
npm install 
sls dynamodb install # or https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.DownloadingAndRunning.html
```

## deploy
```
serverless invoke local -f hello
serverless deploy
serverless invoke -f hello
```
