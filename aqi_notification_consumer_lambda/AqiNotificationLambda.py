from __future__ import print_function
import base64
import json
import boto3
import requests
from requests_aws4auth import AWS4Auth

sns = boto3.client('sns')

region = 'us-west-2'
service = 'es'
credentials = boto3.Session().get_credentials()
aws_auth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

host = 'https://search-second-es-domain-dnykkxhsi4p53l5od5z5ih3lfq.us-west-2.es.amazonaws.com'
index = 'subscriber-index'
doc_type = '_doc'
url = host + '/' + index + '/' + doc_type + '/'
search_url = host + '/' + index + '/' + "_search"
headers = {"Content-Type": "application/json"}

#sending notification to the subscriber notifying regarding pollutant levels
def lambda_handler(event):
    #constants
    number = '+15512341725'
    alert_range = ["Unhealthy for sensitive groups", "Unhealthy", "Very Unhealthy", "Hazardous"]
    ret_document = json.loads(requests.get(url + "1234", auth=aws_auth).text)

    print("Returned document: " + str(ret_document))

    for record in event['Records']:
        # Decode payload from base64 to bytes to JSON to python dict
        payload = base64.b64decode(record["kinesis"]["data"])
        payload_dict = json.loads(payload.decode())

        station_id = payload_dict["StationID"]
        aqi_category = payload_dict["AQI_Category"]
        aqi_value = payload_dict["AQI_Value"]

        print("StationID is "+str(station_id))

        if aqi_category in alert_range:
            message = "the aqi category currently is " + str(aqi_category) + " and AQI value is: " + str(aqi_value)
            Mobile_Numbers=getData(payload_dict["location"])
              # The below condition is used to check whether data found or not
            if(len(Mobile_Numbers['hits']['hits'])==0):
               print("No Subscribers Found")
            else:
                for hit in Mobile_Numbers['hits']['hits']: #loop the data
                    mobile_number = hit['_source']['mobile']

                    print("Subscriber Data\n",hit)
                    # use hit['_source']['<required_filedname>'] to retreive the required feild data from your lambda

                    print("Subscriber Mobile-->",hit['_source']['mobile'])
                    sns.publish(PhoneNumber=mobile_number, Message=message)
                    print(message)

    #print('Successfully processed {} records.'.format(len(event['Records'])))

# Geo distance query to filter subscribers within a specific range to send notification
def getData(loc_dict):
    query={
      "query": {
        "bool": {
          "must": {
            "match_all": {}
          },
          "filter": {
            "geo_distance": {
              "distance": "200km",
              "location": {
                "lat": loc_dict["lat"],
                "lon": loc_dict["lon"]
              }
            }
          }
        }
      }
    }
    result = json.loads(requests.get(search_url, auth=aws_auth, headers=headers, data=json.dumps(query)).text)
    print("it is" + str(result))
    return result