from __future__ import print_function
import base64
import json
import boto3
from aqi import aqi_calculator
from sensor_metadata import sensor_metadata_decorator
from kinesis_writer import batch_put_to_stream

dynamodb = boto3.resource('dynamodb')
dynamoTable = dynamodb.Table('SensorMetadata')

#print('Loading function')
#calls aqi calculator and adds aqi to the payload and again writes it back to the kinsesis stream
def lambda_handler(event):
    payload_list = []

    for record in event['Records']:
        # Decode payload from base64 to bytes to JSON to python dict
        payload=base64.b64decode(record["kinesis"]["data"])
        payload_dict = json.loads(payload.decode())

        # Get dictionary values
        #print(payload_dict)

        payload_list.append(payload_dict)

    sensor_list = sensor_metadata_decorator(dynamodb, payload_list)
    aqi_list = aqi_calculator(sensor_list)

    #print("AQI: "+ str(aqi_list))

    # write to kinesis
    batch_put_to_stream(aqi_list)

    #print('Successfully processed {} records.'.format(len(event['Records'])))