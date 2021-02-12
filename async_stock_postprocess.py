from botocore.exceptions import ClientError
import json
import boto3
import hashlib

# checks dynamodb for file status (in progress, success, or failed)
def lambda_handler(event, context):
    dynamodb = boto3.client('dynamodb')
    file_name = event['unique_hash_id']
    try:
        response = dynamodb.get_item(TableName='stocks_table', Key={'unique_hash_id':{'S': file_name}})
    except ClientError as e:
        return {
            'statusCode': 404, 
            'body': 'Retry process starting with preprocessing lambda'
        }   
    else:
        m_item = response['Item']
    
    m_status = m_item['status']['S']
    
    if m_status == 'In progress':
        return {
            'statusCode': 303,
            'body': 'Work in progress, please poll again to receive status update'
        }
    elif m_status == 'Success':
        return {
            'statusCode': 200,
            'body': {
                'message': 'Completed successfully, find location in s3_file_location key-value pair',
                's3_file_location': m_item['s3_endpoint']['S']
            }
        }
    else:
        return {
            'statusCode': 404,
            'body': 'Retry process starting with preprocessing lambda'
        }
    