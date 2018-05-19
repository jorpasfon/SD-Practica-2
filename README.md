# SD-Practica-2
Pràctica 2 SD - Map-reduce AWS Lambda

## Prerequisites
Access to AWS Lambda with an IAM role through CLI (_aws configure_)
Two AWS S3 buckets, one for the files to _map-reduce_, and another for the results.

## Execution
1. Create a Lambda function for each _mapper_ and _reducer_ you want. You have _mapper.py_ _reducer1.py_ (Word Count) and _mapperDic.py_ _reducer2.py_ (Count Word) as an example.
2. Replace the _mapper_ function to have your own mapper, and the _merge_ function to have your own reducer.
3. On _mainSD2.py_ replace _source_bucket_ and _result_bucket_ variables with the name of your files bucket and result bucket respectively.

