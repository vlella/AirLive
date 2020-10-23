import json
import urllib.parse
import boto3
from decimal import Decimal

print('Loading function')

s3 = boto3.client('s3')
dynamo_db = boto3.resource('dynamodb')
dynamoTable = dynamo_db.Table('SensorMetadata')

#
def lambda_handler(event):
    print("Received event: " + json.dumps(event, indent=2))

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        file_content = response["Body"].read().decode("utf-8-sig")

        print(file_content)

        file_json = json.loads(file_content, parse_float=Decimal)

        print("Number of sensors:"+str(len(file_json)))

        for i in range(9000,11000):

            print(str(file_json[i]))

            dynamoTable.put_item(Item = {
                'sensorID': file_json[i]['sensorID'],
                'country': file_json[i]['country'],
                'city': file_json[i]['city'],
                'Location': file_json[i]['Location']
            })
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e