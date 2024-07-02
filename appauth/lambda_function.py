
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
    print(response)
    return "otp sent"
            
def lambda_handler(event, context):
    print (event)
    query = event['queryStringParameters']
    print(query)
    type = query['type']
    print(type)
    phno = query['number']
    
    response = client.get_item( TableName='user_passwords', Key={"mobile_no": {"S": phno}})

    if type == "login":
        if 'Item' in response:
            if query['password'] == response['Item']['password']['S']:
                return "Success"
            else:
                return "Wrong Password"
        else:
            return "Mobile number not found"

    
    elif type == "new login":
        if 'Item' in response:
            return "Mobile number already exists"
        else:
            return sentOTP(phno,int(event["requestContext"]["timeEpoch"]))
   
   
    elif type == 'forget password':
        if 'Item' in response:
            return sentOTP(phno,int(event["requestContext"]["timeEpoch"]))
        else:
            return "Mobile number not found"
   
    elif type == "otp verify":
        response = client.get_item( TableName='OTP', Key={"mobile_no": {"S": phno}})
        if query['otp'] == response['Item']['password']['S']:
            return "otp verified"
        else:
            return "Wrong OTP"
    
    elif type == "set password":
        data = {
                "mobile_no": {"S": phno},
                "password": {"S": query['password']}
            }
        response=client.put_item(TableName='user_passwords', Item=data)
        return "password set successfull"
    
    elif type == "change_address":
        del query["type"]
        decode= json.loads(query['data'])
        #del decode["password"]
        print("decode =",end="")
        print(decode)
        response=client.put_item(TableName='public_user', Item=PythonToDB(decode))
        # put data to agent cs list
        data={
 "mobile_no": {
  "S": decode['number']
 },
 "address": {
  "S": decode['house_no']
 },
 "location": {
  "S": decode['location']
 },
 "name": {
  "S": decode['fullname']
 },
 "stat": {
  "BOOL": False
 }
}
        response=client.put_item(TableName='12000customer_list', Item=data)
        return "success"
        
    elif type == "customar_signup":
        del query["type"]
        decode= json.loads(query['data'])
        print("decode =",end="")
        print(decode)
        data = {
                "mobile_no": {"S": phno},
                "password": {"S": decode.pop('password')}
            }
        response=client.put_item(TableName='user_passwords', Item=data)
        response=client.put_item(TableName='public_user', Item=PythonToDB(decode))
        data = { "mobile_no": {"S": query["number"]},"coinbalance": {"N": "0"},"thismonth": {"S": "0"},"totalwaste": {"N": "0"},"username": {"S": decode["fullname"]}}
        client.put_item(TableName='user_point_details', Item=data)
        # put data to agent cs list
        data={
 "mobile_no": {
  "S": decode['number']
 },
 "address": {
  "S": decode['house_no']
 },
 "location": {
  "S": decode['location']
 },
 "name": {
  "S": decode['fullname']
 },
 "stat": {
  "BOOL": False
 }
}
        response=client.put_item(TableName='12000customer_list', Item=data)
        
        #creating transac
        tablename=phno+'transaction'
        table_params ={
        'TableName': tablename[3:],
        'BillingMode': 'PAY_PER_REQUEST',  # On-demand billing mode
        'AttributeDefinitions': [
            {
                'AttributeName': 'DateAndTime',  # Use 'AttributeName' instead of 'phno'
                'AttributeType': 'S'  # Assuming a string type for the primary key
            }
        ],
        'KeySchema': [
            {
                'AttributeName': 'DateAndTime',  # Use 'AttributeName' instead of 'phno'
                'KeyType': 'HASH'
            }
        ],
        'Tags': [  # Add tags here
            {
                'Key': 'customer_transaction',
                'Value':''
            }
            ]
        }
        
        response = client.create_table(**table_params)
        return "registration sucessfull"
    
    elif type=="change location":
        del query["type"]
        decode= json.loads(query['data'])
        response = client.update_item(
        TableName='public_user',
        Key={'number': {'S': decode['number']}},
        UpdateExpression='SET location = :val1',
        ExpressionAttributeValues={
            ':val1': {'S': decode['location']}
        }
    )
        response = dynamodb_client.update_item(
        TableName='12000customer_list',
        Key={'mobile_no': {'S': decode['number']}},
        UpdateExpression='SET location = :val1',
        ExpressionAttributeValues={
            ':val1': {'S': decode['location']}
        }
    )
        return "success"        
    return "spelling ekke nokki ayakeda"

    
