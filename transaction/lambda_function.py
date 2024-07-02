import boto3
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
import json
import random
import decimal


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
    
def lambda_handler(event, context):
    print (event)
    query = event['queryStringParameters']
    print(query)
    type = query['type']
    print(type)
    phno = query['number']
    
    if type=="add":
        response = client.get_item( TableName='user_point_details', Key={"mobile_no": {"S": phno}})
        data = DBToPython(response['Item'])
        print(data)
        wei=(query['weight'])
        balance=response['Item']['coinbalance']['N']
        response = client.update_item(
    TableName='user_point_details',
    Key={'mobile_no': {'S': phno}},
    UpdateExpression='SET coinbalance = :val1, thismonth = :val2, totalwaste = :val3',
    ExpressionAttributeValues={
        ':val1': {'N': str(data['coinbalance'] + decimal.Decimal(wei))},
        ':val2': {'N': str(int(data['thismonth']) + int(decimal.Decimal(wei)))},
        ':val3': {'N': str(data['totalwaste'] + decimal.Decimal(wei))}
    }
)
        data={'DateAndTime' : str(event["requestContext"]["timeEpoch"]),'activity' : query['weight'],'in_out': True}
        response=client.put_item(TableName=phno[3:]+'transaction', Item=PythonToDB(data))
        return "Transaction sucessfull"
    
    elif type =="qr":
        response = client.get_item( TableName='12000customer_list', Key={"mobile_no": {"S": phno}})
        if 'Item' in response:
            data=DBToPython(response['Item'])
            if data['stat']:
                return  "active"
            else:
                return "In-active user"
        else:
            return "Wrong user"
        
    elif type == "buy":
        response = client.get_item( TableName='user_point_details', Key={"mobile_no": {"S": phno}})
        dbdata= DBToPython(response['Item'])
        if int(dbdata['coinbalance']) >= int(query['amount']):
            dat={'DateAndTime' : str(event["requestContext"]["timeEpoch"]),'activity' : query['amount'],'in_out': False} 
            print(phno[3:]+'transaction')
            response=client.put_item(TableName=phno[3:]+'transaction', Item=PythonToDB(dat))
            amt=int(query['amount'])
            
            response = client.update_item(
            TableName='user_point_details',
            Key={'mobile_no': {'S': phno}},
            UpdateExpression='SET coinbalance = :val1',
            ExpressionAttributeValues={
                ':val1': {'N': str(dbdata['coinbalance'] - amt)}
            }
            )
            
            return "purchase sucessfull"
        else:
            return "Not enough coins"
    elif type == "map":
        response = client.scan(TableName= phno+'customer_list')
        print(response['Items'])
        return response['Items']
