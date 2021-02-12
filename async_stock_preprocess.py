import json
import boto3
import hashlib
import time

# returns url for polygon api depending on input parameters
def url_handler(event_ticker, event_key, event_range, start_date=None, end_date=None):
    if start_date == None and end_date == None:
        return 'https://api.polygon.io/v2/aggs/ticker/' + event_ticker + '/' + event_range + '?unadjusted=true&apiKey=' + event_key
    else:
        return 'https://api.polygon.io/v2/aggs/ticker/' + event_ticker + '/range/' + event_range + '/day/' + start_date + '/' + end_date + '?unadjusted=true&apiKey=' + event_key


def lambda_handler(event, context):
    # TODO implement
    # print(event)  # for debugging purposes in 
    event_ticker = event['ticker'].upper()
    event_key = event['apiKey']
    event_range = str(event['range'])

    # error checks for invalid characters in inputs that would cause rest call to polygon to fail
    try:
        start_date = event['start_date']
        end_date = event['end_date']
        if ('/' in event_ticker) or ('/' in event_key) or ('/' in event_range) or ('/' in start_date) or ('/' in end_date):
            return {
                'statusCode': 501,
                'body': json.dumps('Input contains forward slashes; please retry with proper input parameters')
            }
        else:
            api_url = url_handler(event_ticker, event_key, event_range, start_date, end_date)
    except:
        if ('/' in event_ticker) or ('/' in event_key) or ('/' in event_range):
            return {
                'statusCode': 501,
                'body': json.dumps('Input contains forward slashes; please retry with proper input parameters')
            }
        else: 
            api_url = url_handler(event_ticker, event_key, event_range)
            
    hash = hashlib.sha1()
    hash.update(str(time.time()).encode('utf-8'))
    file_name = hash.hexdigest()[:15]
    
    # creates row in dynamodb with unique file name 
    db_obj = {
        'unique_hash_id': { 'S': file_name },
        'api_url': { 'S': api_url },
        'status': { 'S': 'In progress' },
        's3_endpoint': { 'S': 'Not yet available'}
    }
    
    
    dynamodb = boto3.client('dynamodb')
    dynamodb.put_item(TableName='stocks_table', Item=db_obj)
    
    # sends user unique file name to use on next polling lambda function
    return {
        'statusCode': 200,
        'body': {
            'unique_hash_id': file_name,
            'second_function_endpoint': 'https://8sq9kk5410.execute-api.us-east-2.amazonaws.com/dev/async_stock_postprocess',
            'message': 'Use unique_hash_id as input field when polling second lambda functions API Gateway (with GET Request) for file upload status in s3'
        }
    }
