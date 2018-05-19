'''
Python mapper function
* Copyright 2016, Amazon.com, Inc. or its affiliates. All Rights Reserved.
*
* Licensed under the Amazon Software License (the "License").
* You may not use this file except in compliance with the License.
* A copy of the License is located at
*
* http://aws.amazon.com/asl/
*
* or in the "license" file accompanying this file. This file is distributed
* on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
* express or implied. See the License for the specific language governing
* permissions and limitations under the License. 
'''

import boto3
import json
import random
import resource
import StringIO
import time

s3 = boto3.resource('s3')
s3_client = boto3.client('s3')

TASK_MAPPER_PREFIX = "task/mapper/";

def mapper(lines):
    countWord = 0    
    for line in lines:
        line = line.lower()
        line = line.replace('.', '').replace(',', '').replace(':', '').replace('"', '').replace('#', '')
        words = line.split()
        for word in words:
            countWord = countWord + 1
    return countWord

def lambda_handler(event, context):
    
    start_time = time.time()

    job_bucket = event['jobresult']
    src_bucket = event['mapreducebucket2']
    filename = event['file']
    id = event['id']
    
    output = {}
    line_count = 0
    err = ''

    s3.meta.client.download_file(src_bucket, filename, '/tmp/hello.txt')
    
    
    with open('/tmp/hello.txt') as f:
        lines = f.readlines()
    
    countWord = mapper(lines)    
    
    with open('/tmp/p1.json', 'w') as f:
        f.write(json.dumps(countWord))
        f.close()
        


    s3.meta.client.upload_file('/tmp/p1.json', job_bucket, id)   

    time_in_secs = (time.time() - start_time)
    return time_in_secs
  

