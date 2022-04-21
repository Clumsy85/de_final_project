import json
import os
import time
import shutil

import pandas as pd
from pyspark.sql import SparkSession

from google_services.def_google_services import sentimental_api,\
    sentimental_api_score

from aws_services.def_boto3_aws import aws_sentimental_analysis, \
    find_all_file_name_in_bucket, \
    read_file_from_s3, \
    aws_sentimental_analysis_score, \
    write_log_dynamodb, \
    aws_upload_dir, \
    aws_call_lambda, \
    aws_refresh_dashboard

from pyspark.sql.functions import udf, StringType

'''
QuickSight paramaters for the dashboard
'''
datasetid = '******************'
ingetionid = '******************'
awsaccountid = '******************'

def create_spark_df(bucket_name, folder_name):
    try:
        # take the bucket name + request_key
        bucket_name = bucket_name
        folder_name = folder_name

        df = pd.DataFrame()
        files = find_all_file_name_in_bucket(bucket_name, folder_name)

        if len(files) == 0:
            return "no data"

        if len(files) == 1:
            content = read_file_from_s3(bucket_name, files[0].split('/')[0], files[0].split('/')[1])
            content = json.loads(content)
            df = pd.DataFrame(content).drop('key_folder', 1).T

        if len(files) > 1:
            content = read_file_from_s3(bucket_name, files[0].split('/')[0], files[0].split('/')[1])
            content = json.loads(content)
            df = pd.DataFrame(content).drop('key_folder', 1).T
            for file in files[1:]:
                content = read_file_from_s3(bucket_name, file.split('/')[0], file.split('/')[1])
                content = json.loads(content)
                df2 = pd.DataFrame(content).drop('key_folder', 1).T
                df = pd.concat([df, df2], ignore_index=True)
# convert to sparkDF
        spark = SparkSession.builder.getOrCreate()
        spark_df = spark.createDataFrame(df)

# define aws and google sentimental analysis spark functions (udf)
        google_sentimental_udf = udf(f=sentimental_api, returnType=StringType())
        google_sentimental_score_udf = udf(f=sentimental_api_score, returnType=StringType())
        aws_sentimental_analysis_udf = udf(f=aws_sentimental_analysis, returnType=StringType())
        aws_sentimental_analysis_score_udf = udf(f=aws_sentimental_analysis_score, returnType=StringType())

# add to the sparkDF the analysis columns
        spark_df = spark_df \
            .withColumn('google_sentimental_vibe', google_sentimental_udf('text')) \
            .withColumn('google_sentimental_score', google_sentimental_score_udf('text')) \
            .withColumn('aws_sentimental_vibe', aws_sentimental_analysis_udf('text')) \
            .withColumn('aws_sentimental_score', aws_sentimental_analysis_score_udf('text'))
        spark_df = spark_df.dropDuplicates()
        spark_df.show(5)

# convert to parquet and upload to s3
        if bucket_name == 'news-data-api':
            spark_df.write.parquet(folder_name + '_news')
            news_folder = folder_name + '_news'
            upload_to_bucket = aws_upload_dir(news_folder, 'request-reports')
            time.sleep(30)
            shutil.rmtree(news_folder)
        if bucket_name == 'twitter-data-api':
            spark_df.write.parquet(folder_name + '_twitter')
            twitter_folder = folder_name + '_twitter'
            upload_to_bucket = aws_upload_dir(twitter_folder, 'request-reports')
            time.sleep(30)
            shutil.rmtree(twitter_folder)
            time.sleep(20)
            initiate_lambda_to_glue = aws_call_lambda('arn:aws:lambda:us-east-1:364366727808:function:lambda-crawler-s3')
            time.sleep(10)
            # initiate_lambda_to_send_sms = aws_call_lambda('arn:aws:lambda:us-east-1:364366727808:function:lambda-sns-glue')

            aws_refresh_dashboard(datasetid, ingetionid, awsaccountid)
        return "The Spark Dataframe has created successfully"
    except Exception as e:
        write_log = write_log_dynamodb('convert data into SparkDF Error', e)
        return f"Error: The Spark Dataframe has not created: {e}"
