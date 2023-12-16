import boto3
import base64
import json
import time
import re
from collections import defaultdict
from datetime import datetime, timedelta

ACCESS_KEY = ''
SECRET_KEY = ''
s1 = boto3.session.Session(aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY, region_name='us-west-2')

lambda_client = s1.client('lambda', region_name='us-west-2')
logs_client = s1.client('logs', region_name='us-west-2')

functionName = 'query'
requestiddict = defaultdict(list)
runtimelst = []

input_data = {
  "filters": {
    "Region": "Europe"
  },
  "aggregations": {
    "groupBy": [
      "Country",
      "`Item Type`"
    ],
    "operations": [
      "SUM(`Units Sold`)",
      "AVG(`Total Profit`)"
    ]
  }
}
json_payload = json.dumps(input_data)


for _ in range(100):
    response = lambda_client.invoke(
        FunctionName=functionName,
        InvocationType='RequestResponse',
        Payload=json_payload
    )

    request_id = response['ResponseMetadata']['RequestId']
    payload = response['Payload'].read()
    decoded_payload = json.loads(payload)
    container_id = decoded_payload.get('containerID')
    print(f'container_id: {container_id}, request_id, {request_id}')
    # print("\nFunction Response Payload:")
    # print(decoded_payload)
    requestiddict[container_id].append(request_id)

time.sleep(10)
print(requestiddict)
print(requestiddict.items())


end_time = int(datetime.now().timestamp() * 1000) 
start_time = end_time - (10 * 60 * 1000) 

for container_id, requestidlst in requestiddict.items():

    log_group_name = '/aws/lambda/' + functionName
    log_stream_name = container_id

    events = logs_client.get_log_events(
        logGroupName=log_group_name,
        logStreamName=log_stream_name,
        startTime=start_time,
        endTime=end_time
    )

    for event in events['events']:
        for request_id in requestidlst:
            # print(request_id, event['message'])
            if request_id in event['message']:
                match = re.search(r'Billed Duration: (\d+) ms', event['message'])
                if match:
                    billed_duration = int(match.group(1))
                    print(billed_duration)
                    runtimelst.append([request_id, billed_duration])

print(runtimelst)

print(sum([i[1] for i in runtimelst]) / 50)
