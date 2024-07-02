import boto3
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
import json
import random

client = boto3.client('dynamodb')

def PythonToDB(python_obj: dict) -> dict:
    serializer = TypeSerializer()
    return {
        k: serializer.serialize(v)
        for k, v in python_obj.items()
    }

def DBToPython(dynamo_obj: dict) -> dict:
    deserializer = TypeDeserializer()
    return {
        k: deserializer.deserialize(v) 
        for k, v in dynamo_obj.items()
    }
    
def sentOTP(no,time):
    exptime=(time//1000)+300
    random_number = str(random.randint(1000, 9999))
    data = {
            "mobile_no": {"S": no},
            "password": {"S": random_number},
            "Expire_otp": { "N": str(exptime)}
           }
    response=client.put_item(TableName='OTP', Item=data)
    print("sms respopnce = ",end="")
    print(response)
    sns = boto3.client('sns')
    message = 'Your OTP is ' + random_number
    sns.publish(Message=message, PhoneNumber=no)
    return "otp sent"

def lambda_handler(event, context):
    print (event)
    query = event['queryStringParameters']
    print(query)
    type = query['type']
    print(type)
    IDno = query['number']
    print ( IDno)
    response = client.get_item( TableName='agent_details', Key={"id": {"S": IDno}})
    response = client.get_item( TableName='user_passwords', Key={"mobile_no": {"S": IDno}})
    if type == "login":
        response = client.get_item( TableName='user_passwords', Key={"mobile_no": {"S": IDno}})
        if 'Item' in response:
            if query['password'] == response['Item']['password']['S']:
                return "Success"
            else:
                return "Wrong Password"
        else:
            return "ID not found"
    
    elif type == "test":
        print("test responce =",end = "")
        print(response)
        if 'Item' in response:
            return "otp sent"
        else:
            return "ID not found"
            
    elif type == "new login":
        if 'Item' in response:
            return sentOTP(query['mob'],int(event["requestContext"]["timeEpoch"]))
        else:
            return "ID not found"
    
    elif type == "otp verify":
        response = client.get_item( TableName='OTP', Key={"mobile_no": {"S": query['number']}})
        if query['otp'] == response['Item']['password']['S']:
            return "otp verified"
        else:
            return "Wrong OTP"
    
    elif type == "set password":
        data = {
                "mobile_no": {"S": IDno},
                "password": {"S": query['password']}
            }
        response=client.put_item(TableName='user_passwords', Item=data)
        return "password set successfull"
    
    elif type == "customar_signup":
        decode= json.loads(query['data'])
        print("decode =",end="")
        print(decode)
        data = {
                "mobile_no": {"S": IDno},
                "password" : {"S":decode.pop('password')}
            }
        response=client.put_item(TableName='user_passwords', Item=data)
        response=client.put_item(TableName='agent_details', Item=PythonToDB(decode))
        
        
        # coustomer list db
        table_params ={ 
        'TableName': IDno+'customer_list',
        'BillingMode': 'PAY_PER_REQUEST',  # On-demand billing mode
        'AttributeDefinitions': [
            {
                'AttributeName': 'mobile_no',  # Use 'AttributeName' instead of 'phno'
                'AttributeType': 'S'  # Assuming a string type for the primary key
            }
        ],
        'KeySchema': [
            {
                'AttributeName': 'mobile_no',  # Use 'AttributeName' instead of 'phno'
                'KeyType': 'HASH'
            }
        ],
        'Tags': [  # Add tags here
            {
                'Key': 'Agent_customer_list',
                'Value':''
            }
            ]
        }
        response = client.create_table(**table_params)
        
        #collection history
        table_params ={ 
        'TableName': IDno+'history',
        'BillingMode': 'PAY_PER_REQUEST',  # On-demand billing mode
        'AttributeDefinitions': [
            {
                'AttributeName': 'time',  # Use 'AttributeName' instead of 'phno'
                'AttributeType': 'N'  # Assuming a string type for the primary key
            }
        ],
        'KeySchema': [
            {
                'AttributeName': 'time',  # Use 'AttributeName' instead of 'phno'
                'KeyType': 'HASH'
            }
        ],
        'Tags': [  # Add tags here
            {
                'Key': 'Agent_history',
                'Value':''
            }
            ]
        }
        response = client.create_table(**table_params)

        response = client.get_item( TableName='area', Key={"pincode": {"S": decode['pin_no']}})
        if 'Item' in response:
            data = DBToPython(response['Item'])
            print("DB data = ",end ="")
            print(data['IDno'])
            data['IDno'].add(decode['id'])
            response = client.update_item(
            TableName='area',
            Key={'pincode': {'S': decode['pin_no']}},
            UpdateExpression='SET IDno = :val1',
            ExpressionAttributeValues={
            ':val1': {'SS': list(data['IDno'])}
        }
        )
        else:
            lst=[]
            lst.append(decode['id'])
            data={"pincode": {"S": decode['pin_no']},
            "IDno": {"SS": lst}}
            response=client.put_item(TableName='area', Item=data)
        
        return "registration sucessfull"    