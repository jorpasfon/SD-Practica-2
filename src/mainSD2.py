import boto3, json, time

num = raw_input("Escolleix la funcio de reduce 1 = Total paraules, 2 = numero paraules")
concatenacio = ""
lambda_client = boto3.client('lambda')
client = boto3.client('s3')
s3 = boto3.resource('s3')

totsfitxers = True
totalmappers = raw_input("Diguem el nombre de mappers")

source_bucket = 'mapreducebucket3'
result_bucket = 'jobresult2'

if num == '1':

    for x in range(int(totalmappers)) :
        test_event = {'file': 'file.txt', 'mapreducebucket2': source_bucket, 'jobresult': result_bucket, 'id':'dic'+str(x+1)+'.json'}

        lambda_client.invoke(
            FunctionName='mapperDic',
            InvocationType='Event',
            Payload=json.dumps(test_event),
        )
    cont = 0
    my_bucket = s3.Bucket('jobresult2')
    while totsfitxers:

        for object in my_bucket.objects.filter(Prefix='dic'):


            concatenacio += object.key+" "
            cont+=1

        if (cont >= int(totalmappers)):
            totsfitxers = False
        else:
            time.sleep(1)
        print concatenacio
        print concatenacio[:-1]



    test_event = { "totallist":concatenacio[:-1] ,"jobresult": result_bucket}
    lambda_client.invoke(
        FunctionName='Reducer1',
        InvocationType='Event',
        Payload=json.dumps(test_event),
    )
else:

    for x in range(int(totalmappers)) :
        test_event = {'file': 'file.txt', 'mapreducebucket2': source_bucket, 'jobresult': result_bucket, 'id':'p'+str(x+1)+'.json'}

        lambda_client.invoke(
            FunctionName='mapper',
            InvocationType='Event',
            Payload=json.dumps(test_event),
        )
    cont = 0
    my_bucket = s3.Bucket('jobresult2')
    while totsfitxers:

        for object in my_bucket.objects.filter(Prefix='p'):


            concatenacio += object.key+" "
            cont+=1

        if (cont >= int(totalmappers)):
            totsfitxers = False
        else:
            time.sleep(1)
        print concatenacio

    test_event = {'jobresult': result_bucket, 'totallist': concatenacio[:-1]}
    lambda_client.invoke(
        FunctionName='Reducer2',
        InvocationType='Event',
        Payload=json.dumps(test_event),
    )



