import urllib3
import json
import boto3
import time
import hashlib

dynamodb = boto3.client('dynamodb')

# handles errors resulting from api call to polygon
def error_handler(api_json, api_status, hash_id):
    if api_status == 401:  # returns with 401 error if api key is invalid
        dynamodb.update_item(
            TableName = 'stocks_table',
            Key={ 'unique_hash_id': { 'S': hash_id } },
            UpdateExpression="SET #ts = :mystatus",
            ExpressionAttributeValues={ ":mystatus": { "S": "Failed - 401" } },
            ExpressionAttributeNames={ "#ts": "status" })
        return 'ERROR'
    elif api_json['resultsCount'] == 0:  # returns 204 on no stats for ticker
        dynamodb.update_item(
            TableName = 'stocks_table',
            Key={ 'unique_hash_id': { 'S': hash_id } },
            UpdateExpression="SET #ts = :mystatus",
            ExpressionAttributeValues={ ":mystatus": { "S": "Failed - 204" } },
            ExpressionAttributeNames={ "#ts": "status" })
        return 'ERROR'
    return 'OK'


# calls polygon api, does computations, inserts into s3, and updates to dynamodb
def lambda_handler(event, context):
    if event['Records'][0]['eventName'] != 'INSERT':
        return -1
        
    # return on updates (lambda function triggers, but payload will not have 
    # 'api_url' unless new entry with api_url is modified since api_url is a 
    # static entry in db through whole process) - prevents infinite loop 
    # triggers from dynamodb to this lambda
    try:
        api_url = event['Records'][0]['dynamodb']['NewImage']['api_url']['S']
    except:
        return -1

    hash_id = event['Records'][0]['dynamodb']['NewImage']['unique_hash_id']['S']  # update with multiple records at once
    hash_json = hash_id + '.json'
    
    http = urllib3.PoolManager()
    api_res = http.request('GET', api_url)
    api_status = api_res.status
    api_json = json.loads(api_res.data)

    if error_handler(api_json, api_res.status, hash_id) == 'ERROR':  # error checking that could be implemented with more time
        return -1
    
    # if none of the error codes are tripped, proceed to main logic
    try: 
        res_list = api_json['results']
    except KeyError:
        dynamodb.update_item(
            TableName = 'stocks_table',
            Key={ 'unique_hash_id': { 'S': hash_id } },
            UpdateExpression="SET #ts = :mystatus",
            ExpressionAttributeValues={ ":mystatus": { "S": "Failed - 204" } },
            ExpressionAttributeNames={ "#ts": "status" })
        return -1
    else:  # run if there are results in api_json
        min_sp = min(i['l'] for i in res_list)
        max_sp = max(i['h'] for i in res_list)
        min_vol = min(i['v'] for i in res_list)
        max_vol = max(i['v'] for i in res_list)
        total_vol = 0
        total_price = 0
        for i in res_list:
            total_vol += float(i['v'])
            total_price += float(i['vw']) * float(i['v'])
        avg_sp = float(total_price/total_vol)
        
        result = {
            'Ticker': api_json['ticker'],
            'Min_Stock_Price': min_sp,
            'Avg_Stock_Price': avg_sp,
            'Max_Stock_Price': max_sp,
            'Min_Volume': min_vol,
            'Max_Volume': max_vol
        }
    
    # push file to s3 and update dynamodb that the job was successful
    client = boto3.resource('s3')
    object = client.Object('asyncstockbucket', hash_json)
    object.put(Body=(bytes(json.dumps(result).encode('UTF-8'))))
    s3_url = 'https://asyncstockbucket.s3.amazonaws.com/' + hash_json
    
    dynamodb.update_item(
        TableName = 'stocks_table',
        Key={ 'unique_hash_id': { 'S': hash_id } },
        UpdateExpression="SET #ts = :mystatus, #surl = :mysurl",
        ExpressionAttributeValues={ ":mystatus": { "S": "Success" },
                                    ":mysurl": { "S": s3_url }},
        ExpressionAttributeNames={ "#ts": "status",
                                    "#surl": "s3_endpoint"})
    
    return {
        'statusCode': 200,
        'body': 'file has been uploaded to s3'
    }
