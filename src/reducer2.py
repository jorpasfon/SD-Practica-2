
import boto3
import resource
import StringIO
import json
import time

def merge(dict1, dict2):
    return dict1 + dict2

def lambda_handler(event, context):
    
    start_time = time.time()
    
    s3 = boto3.resource('s3')
    s3_client = boto3.client('s3')
    
    llista = list()
    
    job_bucket = event['jobresult']
    job_list = event['totallist']
    
    listT = job_list.split(" ")
    
    for i in listT:
        s3.meta.client.download_file(job_bucket, i, '/tmp/'+i)
        print '/tmp/'+i
        with open('/tmp/'+i) as f:
            data = json.load(f)
            llista.append(data)
    
    result = reduce(merge, llista)
    with open('/tmp/nouDiccF4.txt', 'w') as f:
        f.write(json.dumps(result))
        f.close()    
    
    s3.meta.client.upload_file('/tmp/nouDiccF4.txt', job_bucket, 'resultNumeric.txt')
    
    time_in_secs = (time.time() - start_time)
    return time_in_secs

