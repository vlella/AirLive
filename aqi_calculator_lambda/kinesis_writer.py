import boto3
import json

# The kinesis stream to which we are writing from aqi lambda
stream_name = 'AqiDecoratedStream'

k_client = boto3.client('kinesis', region_name='us-west-2')

# Batch Put logic
def batch_put_to_stream(aqi_list):

        print ("Payload from writer: "+str(aqi_list))

        records = []
        for aqi_dict in aqi_list:
                record = dict()
                record['Data'] = json.dumps(aqi_dict)
                record['PartitionKey'] = str(aqi_dict['StationID'])
                records.append(record)

        print("Records: "+str(records))

        put_response = k_client.put_records(
                        StreamName=stream_name,
                        Records=records)
        print("Successfully written to kinesis: "+str(put_response))