import json
import boto3
import pandas as pd
import yfinance as yf

kinesis = boto3.client('kinesis', 'us-east-2')

tickers = ["FB", "SHOP", "BYND", "NFLX", "PINS", "SQ", "TTD", "OKTA", "SNAP", "DDOG"]

def lambda_handler(event, context):
    for ticker in tickers:
        data = yf.download(ticker, start = "2021-11-30", end = "2021-12-01", interval = "5m")
        data = data.dropna()
    
        # iterate through each row
        for index, row in data.iterrows():
            record = {"high" : row['High'], "low" : row['Low'], "ts" : str(index), "name" : ticker}
            
            # converting record to json
            json_row = json.dumps(record) + "\n"
            
            # placing data into delivery stream
            kinesis.put_record(
                StreamName = "STA9760-Project03",
                Data = json_row,
                PartitionKey = "partitionkey")
    
    return {
        'statusCode': 200,
        'body': "Done!"
    }
