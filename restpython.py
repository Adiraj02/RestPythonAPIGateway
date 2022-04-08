import json
import boto3
from botocore.exceptions import ClientError

client = boto3.client('dynamodb')

def lambda_handler(event, context):
    if event['httpMethod'] == 'POST':
        request = json.loads(event['body'])
        try:
            client.put_item( TableName ='Employee',
                Item={
                    'EID': {
                        'S': request['EID']
                    },
                    'FirstName': {
                        'S': request['FirstName']
                        },
                    'LastName': {
                        'S': request['LastName']
                        }
                    },
                    ConditionExpression='attribute_not_exists(EID)'
                )
            return custom_response(201, f"Successfully created employee {request['EID']}")
        except ClientError as e:
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                return custom_response(409, f"Employee with id {request['EID']} already exists. Use 'PUT' to perform record updation")
        except:
            return custom_response(400, 'An error occured while creating the employee :(')
    elif event['httpMethod'] == 'PUT':
        request = json.loads(event['body'])
        try:
            client.put_item( TableName ='Employee',
                Item={
                    'EID': {
                        'S': request['EID']
                    },
                    'FirstName': {
                        'S': request['FirstName']
                        },
                    'LastName': {
                        'S': request['LastName']
                        }
                    },
                    ConditionExpression='attribute_exists(EID)'
                )
            return custom_response(200, f"Successfully updated employee {request['EID']}")
        except ClientError as e:
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                return custom_response(400, f"Employee with id {request['EID']} doesn't exists. Use 'POST' to perform record creation")
        except:
            return custom_response(400, 'An error occured while updating the employee :(')
    elif event['httpMethod'] == 'GET':
        if event['queryStringParameters'] == None:
            data = client.scan(TableName = 'Employee')
            return custom_response(200, data['Items'])
        else:
            try:
                data = client.get_item( TableName='Employee',
                                    Key={
                                        'EID': {
                                            'S': event['queryStringParameters']['EID']
                                            }
                                        }
                                )
                if "Item" not in data.keys():
                    return custom_response(404, 'Opps, No such record in DynamoDB :(')
                else:
                    return custom_response(200, data['Item'])
            except:
                return custom_response(400, f"Incorrect Key. Expected EID but got {list(event['queryStringParameters'].keys())[0]} :(")
    elif event['httpMethod'] == 'DELETE':
        try:
            data = client.delete_item( TableName='Employee',
                                    Key={
                                        'EID': {
                                            'S': event['queryStringParameters']['EID']
                                            }
                                        },
                                        ConditionExpression='attribute_exists(EID)'
                                )
            return custom_response(200, f"Successfully deleted employee with id {event['queryStringParameters']['EID']}")
        except ClientError as e:
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                return custom_response(404, f"Employee with id {event['queryStringParameters']['EID']} not found :(")
        except:
            return custom_response(400, 'An error occured while deleting the employee :(')

def custom_response(code, message):
    response = {
        'statusCode': code,
        'body': json.dumps({"message":message})
    }
    return response