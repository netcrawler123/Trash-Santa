import json
import boto3
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer

def DBToPython(dynamo_obj: dict) -> dict:
    deserializer = TypeDeserializer()
    return {
        k: deserializer.deserialize(v) 
        for k, v in dynamo_obj.items()
    }
    
def PythonToDB(python_obj: dict) -> dict:
    serializer = TypeSerializer()
    return {
        k: serializer.serialize(v)
        for k, v in python_obj.items()
    }
    
client = boto3.client('dynamodb')

def lambda_handler(event, context):
    data = event['queryStringParameters']
    print(event)
    print(data)
    page = data["page"]
    phno = data["number"]
    if page=="address":
        response = client.get_item( TableName='public_user', Key={"number": {"S": phno}})
        return  DBToPython(response['Item'])
    elif page =="points":
        response = client.get_item( TableName='user_point_details', Key={"mobile_no": {"S": phno}})
        return  DBToPython(response['Item'])
    elif page == "map":
        response = client.scan(TableName= phno+'customer_list')
        print(response['Items'])
        return response['Items']
    elif page == "data":
        response = client.get_item( TableName='agent_homescrn', Key={"id": {"S": phno}})
        print(response['Item'])
        return  DBToPython(response['Item'])
   