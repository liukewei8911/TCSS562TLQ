import boto3
import base64
import json
import time
import re


ACCESS_KEY = ''
SECRET_KEY = ''
s1 = boto3.session.Session(aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY, region_name='us-east-2')

lambda_client = s1.client('lambda')
logs_client = s1.client('logs')

functionName = 'Loader'
requestidlst = []
runtimelst = []

for _ in range(50):
    response = lambda_client.invoke(
        FunctionName=functionName,
        InvocationType='RequestResponse'
    )

    request_id = response['ResponseMetadata']['RequestId']
    payload = response['Payload'].read()
    decoded_payload = json.loads(payload)
    container_id = decoded_payload.get('containerID')
    print("\nFunction Response Payload:")
    print(decoded_payload)
    requestidlst.append(request_id)

time.sleep(10)
log_group_name = '/aws/lambda/' + functionName
log_stream_name = container_id

events = logs_client.get_log_events(
    logGroupName=log_group_name,
    logStreamName=log_stream_name
)

for event in events['events']:
    for request_id in requestidlst:
        if request_id in event['message']:
            match = re.search(r'Billed Duration: (\d+) ms', event['message'])
            if match:
                billed_duration = int(match.group(1))
                print(billed_duration)
                runtimelst.append([request_id, billed_duration])

print(runtimelst)

print(sum([i[1] for i in runtimelst]) / 50)
