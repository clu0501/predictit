import json
import requests
import boto3

def json_scraper(url, file_name, bucket):
    print('start_running')
    response = requests.request("GET", url)
    json_data=response.json()
    
    with open(file_name, 'w', encoding='utf-8') as json_file:
        json.dump(json_data, json_file, ensure_ascii=False, indent=4)
        
    s3 = boto3.client('s3')
    s3.upload_file(file_name, bucket, f"predictit/{file_name}")
    
    print('end running')
    
if __name__ == '__main__':
    json_scraper('https://www.predictit.org/api/marketdata/all/', 'predictit_market.json', 'clu0501-predictit-raw')