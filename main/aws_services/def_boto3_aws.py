import boto3
from datetime import datetime
import json
from io import StringIO
import os

aws_access_key_id_user = '*********************'
aws_secret_access_key_user = '*********************'
api_news_data_key = '*********************'


def get_aws_key():
    return ([aws_access_key_id_user, aws_secret_access_key_user])


aws = get_aws_key()


# Since we configured the aws cli we do not have to use the connect_to_s3 function
def connect_to_s3():
    s3 = boto3.client(
        service_name='s3',
        region_name='us-east-1',
        aws_access_key_id=aws[0],
        aws_secret_access_key=aws[1]
    )

    return s3


# when exception occur, send log record rto dynamodb
def write_log_dynamodb(process_name, log_exception):
    try:
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table('error_logs')
        item = {
            'process_name': process_name,
            'exception': str(log_exception),
            'date': datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
        }
        table.put_item(Item=item)
        return "write error log - success"
    except Exception as e:
        return f"Can't upload error log to dynamoDB: {e}"


def create_folder_in_s3(bucket_name, directory_name):
    s3 = connect_to_s3()
    bucket_name = bucket_name
    directory_name = directory_name  # it's name of your folders
    s3.put_object(Bucket=bucket_name, Key=(directory_name + '/'))


def read_file_from_s3(bucket_name, folder_name, file_name):
    s3 = connect_to_s3()
    bucket_name = bucket_name
    folder_name = folder_name
    file_name = file_name
    file_to_read = folder_name + '/' + file_name
    s3 = connect_to_s3()

    result = s3.get_object(Bucket=bucket_name, Key=file_to_read)
    text = result["Body"].read().decode()

    return (text)


# to use kinesis data stream - eventually, we haven't used data stream but only firehose with a direct put
def kinesis_request_stream(stram_name, file_object, bucket_name, folder_name, file_name):
    try:
        s3 = connect_to_s3()
        file_object = file_object
        bucket_name = bucket_name
        folder_name = folder_name
        file_name = file_name
        kinesis = boto3.client("kinesis")
        kinesis.put_record(
            StreamName=stram_name,
            Body=file_object,
            Bucket=bucket_name,
            PartitionKey="partitionkey",
            Key=(folder_name + '/' + file_name)
        )
        return "Kinesis is working"
    except Exception as e:
        write_log_dynamodb('kinsesis_request_stream', e)
        return f"kinesis firehose have failed : {e}"


# for kinesis firehose
def firehose_to_s3(stream_name, object):
    try:
        kinesis = boto3.client("firehose")
        result = kinesis.put_record(DeliveryStreamName=stream_name,
                                    Record={'Data': object})
        return ("s3 upload success")
    except Exception as e:
        write_log_dynamodb('firehose_to_s3', e)
        return f"upload to s3 failed: {e}"


# sentimental analysis by aws - vibe
def aws_sentimental_analysis(txt):
    try:
        client = boto3.client('comprehend')
        text = txt
        response = client.detect_sentiment(
            Text=text,
            LanguageCode='en'
        )
        sentiment = response["Sentiment"].capitalize()
        sentiment_score = (response["SentimentScore"][sentiment])

        return sentiment
    except Exception as e:
        write_log_dynamodb('aws_sentimental_analysis', e)
        return f"something went wrong in aws sentimental analysis: {e}"


# sentimental analysis by aws - score
def aws_sentimental_analysis_score(txt):
    try:
        client = boto3.client('comprehend')
        text = txt
        response = client.detect_sentiment(
            Text=text,
            LanguageCode='en'
        )
        sentiment = response["Sentiment"].capitalize()
        sentiment_score = (response["SentimentScore"][sentiment])

        return sentiment_score
    except Exception as e:
        write_log_dynamodb('aws_sentimental_analysis_score', e)
        return f"something went wrong in aws sentimental analysis: {e}"


# go through a specific bucket in s3 and find all the file names
def find_all_file_name_in_bucket(bucket_name, folder_name):
    try:
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(bucket_name)
        prefix_objs = bucket.objects.filter(Prefix=folder_name + '/')
        files_names = []
        for obj in prefix_objs:
            files_names.append(obj.key)
        return files_names
    except Exception as e:
        write_log_dynamodb('find_all_file_name_in_bucket', e)
        return f"something went wrong in aws find_all_file_name_in_bucket: {e}"


def upload_folder_to_s3(file_object, bucket_name):
    try:
        s3 = connect_to_s3()
        file_object = file_object
        bucket_name = bucket_name
        s3.put_object(
            Body=file_object,
            Bucket=bucket_name
        )
        return 'upload to s3 - success'
    except Exception as e:
        write_log_dynamodb('upload_folder_to_s3', e)
        return f"upload file to s3 with boto3 failed: {e}"


def upload_file_to_s3(file_object, bucket_name, folder_name, file_name):
    try:
        s3 = connect_to_s3()
        file_object = file_object
        bucket_name = bucket_name
        folder_name = folder_name
        file_name = file_name
        s3.put_object(
            Body=file_object,
            Bucket=bucket_name,
            Key=(folder_name + '/' + file_name)
        )
        return 'upload to s3 - success'
    except Exception as e:
        write_log_dynamodb('upload file to s3', e)
        return f"upload file to s3 with boto3 failed: {e}"


def aws_upload_dir(local_dir, bucket_name):
    try:
        s3 = boto3.resource('s3')
        for subdir, dirs, files in os.walk(local_dir):
            for file in files:
                s3.meta.client.upload_file(subdir + '/' + file, bucket_name, subdir + '/' + file)
        return "Directory has uploaded successfully"
    except Exception as e:
        return f"Error - directory has not uploaded: {e}"


def aws_call_lambda(lambda_function_param):
    try:
        client = boto3.client('lambda')
        client.invoke(FunctionName=lambda_function_param, LogType='None')
        return "lambda ran successfully"
    except Exception as e:
        return f"Can't run lambda error log to dynamoDB: {e}"


def aws_refresh_dashboard(datasetid, ingetionid, awsaccountid):
    try:
        client = boto3.client('quicksight')
        client.create_ingestion(DataSetId=datasetid, IngestionId=ingetionid, AwsAccountId=awsaccountid)
        return "Dashboard refresh - success"
    except Exception as e:
        return f"Can't refresh dashboard : {e}"
