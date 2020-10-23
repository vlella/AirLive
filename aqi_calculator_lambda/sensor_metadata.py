#gets the sensor metadata added to the payload
def sensor_metadata_decorator(dynamodb, payload_list):
    decorated_payload_list=payload_list.copy()
    keys=[]
    unique_keys = set()
    for payload_dict in payload_list:
        for i,j in payload_dict.items():
            if i=="StationID":
                if j not in unique_keys:
                    unique_keys.add(j)
                    keys.append({'sensorID': j})

    response = dynamodb.batch_get_item(
        RequestItems={
            'SensorMetadata': {
                'Keys': keys
            }
        }
    )

    response_dict = {}
    for i in response['Responses']['SensorMetadata']:
        response_dict[i['sensorID']] = i

    for decorated_payload_dict in decorated_payload_list:
        station_id = decorated_payload_dict['StationID']

        decorated_payload_dict["location"] = {
            "lat": float(response_dict[station_id]['Location']['lat']),
            "lon": float(response_dict[station_id]['Location']['lon'])
        }
        decorated_payload_dict["city"]= response_dict[stationid]['city']
        decorated_payload_dict["country"]= response_dict[stationid]['country']

        print("Decorated payload"+str(decorated_payload_dict))

    return decorated_payload_list