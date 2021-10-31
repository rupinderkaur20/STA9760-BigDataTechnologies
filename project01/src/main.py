import argparse
import sys
import os
from datetime import datetime
from sodapy import Socrata
## to connect to elastic search
import requests
from requests.auth import HTTPBasicAuth

##allows you to enter your credentials on command line, rather than having them on the script
DATASET_ID = os.environ["DATASET_ID"]
APP_TOKEN = os.environ["APP_TOKEN"]
INDEX_NAME = os.environ["INDEX_NAME"]
ES_HOST = os.environ["ES_HOST"]
ES_USERNAME = os.environ["ES_USERNAME"]
ES_PASSWORD = os.environ["ES_PASSWORD"]

#argparse
parser = argparse.ArgumentParser(description = "Process data from violations")
parser.add_argument("--page_size", type = int, help = "How many rows to fetch per page?", required= True)
parser.add_argument("--num_pages", type = int, help = "How many pages to fetch?", default = 1)
args = parser.parse_args(sys.argv[1:])
print(args)


if __name__ == "__main__":
    ## creating a new ElasticSearch index
    try:
        resp = requests.put(
            f"{ES_HOST}/{INDEX_NAME}",
            auth = HTTPBasicAuth(ES_USERNAME, ES_PASSWORD),
            json = {
                "settings":{
                    "number_of_shards" : 5,
                    "number_of_replicas": 1
                },
                "mappings": {
                    "properties": {
                        "issue_date": {"type": "date"},
                        "plate": {"type": "Keyword"},
                        "license_type": {"type": "Keyword"},
                        "summons_number": {"type": "Keyword"},
                        "state": {"type": "Keyword"},
                        "county": {"type": "keyword"}, 
                        "precinct": {"type": "Keyword"},
                        "violation": {"type": "Keyword"},
                        "fine_amount": {"type": "float"}
                    }
                }
            }
        )
        resp.raise_for_status()
        
    except Exception as e:
        print("Index already exits!")
        
    #getting the rows
    client = Socrata(
       "data.cityofnewyork.us", 
        APP_TOKEN,
    )
    
    for page in range(0,args.num_pages):
        rows = client.get(DATASET_ID, limit= args.page_size, order = "summons_number", offset =page*args.page_size) # rows
        for row in rows:
        
            try:
                es_row = {}
                es_row["issue_date"] = datetime.strptime(row["issue_date"], '%M/%d/%Y').strftime('%Y-%m-%d') # not pushing rows to elastic search with issue date missing
                
                # this plate number seemed to be a "test run" number-> all the fine amount here are zero
                if row["plate"] == "9999999999": 
                    continue
                elif "plate" in row:    
                    es_row["plate"] = row["plate"]
                else:
                    es_row["plate"] = "NA"
                    
                if "license_type" in row:    
                    es_row["license_type"] = row["license_type"]
                else:
                    es_row["license_type"] = "NA"
                    
                if "summons_number" in row:    
                    es_row["summons_number"] = row["summons_number"]
                else:
                    es_row["summons_number"] = "NA"
                    
                if "state" in row:    
                    es_row["state"] = row["state"]
                else:
                    es_row["state"] = "NA"
                    
                if "county" in row:  
                    es_row["county"] = row["county"]
                else:
                    es_row["county"] = "NA"
                    
                if "precinct" in row:    
                    es_row["precinct"] = row["precinct"]
                else:
                    es_row["precinct"] = "NA"
                    
                if "violation" in row:    
                    es_row["violation"] = row["violation"]
                else:
                    es_row["violation"] = "NA"
                    
                if "fine_amount" in row:
                    es_row["fine_amount"] = float(row["fine_amount"])
                else: 
                    es_row["fine_amount"] = 0.0
                    
                
            except Exception as e:
                print(f"FAILED! error is: {e}")
                continue #skips when an error is encoutered
            
            ## pushing rows to elastic search
            try:
                resp = requests.post(
                    f"{ES_HOST}/{INDEX_NAME}/_doc",
                    auth = HTTPBasicAuth(ES_USERNAME, ES_PASSWORD),
                    json = es_row
                )
                resp.raise_for_status()
            except Exception as e:
                print(f"Failed to upload to elasticsearch! {e}")
     
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            