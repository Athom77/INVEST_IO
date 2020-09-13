###### INVESTIO glue job script - news_investing

import sys
import json
import pyspark
from pyspark.sql.functions import col, collect_list, array_join, udf

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from datetime import datetime

import boto3

bucket_str = 'invest-io'
folder_name = 'news_investing'
prefix_str = 'sr-invest-io-all-raw/' + folder_name

client = boto3.client('s3')
response = client.list_objects(Bucket=bucket_str, Prefix=prefix_str)

## READ PARAMETERS
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

## START JOB CONTEXT AND JOB
sc = SparkContext()


glueContext = GlueContext(sc)
spark = glueContext.spark_session
    
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

s3_resource = boto3.resource('s3')

for idx in range(1,len(response['Contents'])):
    
    print(str(idx) + " " + response['Contents'][idx]['Key'])
    
    old_file_name = response['Contents'][idx]['Key']
    old_file_name_complete = 's3://' + bucket_str + '/' + old_file_name
    new_file_name_short = old_file_name.replace(prefix_str + '/','')
    new_file_name = prefix_str + '/lock_' + new_file_name_short
    new_file_name_complete = 's3://' + bucket_str + '/' + new_file_name
    
    if 'lock_' not in old_file_name:
        
        ##### RINOMINA suffissi
        s3_resource.Object(bucket_str, new_file_name).copy_from(
         CopySource=bucket_str + '/' + old_file_name)
        #s3_resource.Object(bucket_str, old_file_name).delete()
        
        
        #### READ INPUT FILES TO CREATE AN INPUT DATASET
        df = spark.read \
            .option("header","true") \
            .option("quote", "\"") \
            .option("escape", "\"") \
            .csv(new_file_name_complete)
            
        df.show()
        
        #convert date
        from pyspark.sql.functions import udf
        from pyspark.sql.types import StringType, FloatType, IntegerType, DateType
        
        def convert_date(x):
            str_reform = datetime.strptime(x, '%d/%m/%Y')
            str_reform_2 = str_reform.strftime('%Y-%m-%d')
            return str_reform_2
        conv_date_udf = udf(lambda z: convert_date(z), StringType())
        
        def trim_news(x):
            if x is not None:
                return x.strip('[]" ''').replace('/','').replace('\'','')
            else:
                return ''

        trim_news_udf = udf(lambda x: trim_news(x), StringType())
        
        df.select("date", conv_date_udf("date").alias("new_date")).show()
        
        #,news_id,ticker,date,time_stamp,author,title,link,article
        
        df2 = df.select("news_id", conv_date_udf("date").alias("date formatted"), "ticker", "author", "title", "link", trim_news_udf("article").alias("article cleaned") )
        
        df2.show()
        df2.printSchema()
        bucket_prod_str = '/sr-investio-all-dwh/' + folder_name + '/'

        df2.toPandas().to_csv('s3://' + bucket_str + bucket_prod_str + new_file_name_short)
        
        print("\n \n  ___________!!!!!____________ Completed loop number " + str(idx))
        
    print("\n done")