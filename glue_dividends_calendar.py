###### INVESTIO glue job script - dividends calendar

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
folder_name = 'dividends-calendar'
prefix_str = 'sr-invest-io-all-raw/' + folder_name

client = boto3.client('s3')
response = client.list_objects(Bucket=bucket_str, Prefix=prefix_str)

###### READ PARAMETERS
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

##### START JOB CONTEXT AND JOB
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
        s3_resource.Object(bucket_str, old_file_name).delete()
        
        
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
            try:
                str_reform = datetime.strptime(x, '%b %d, %Y')
                str_reform_2 = str_reform.strftime('%Y-%m-%d')
                return str_reform_2
            except:
                return x
        conv_date_udf = udf(lambda z: convert_date(z), StringType())
        
        df.select("Ex-dividend date", conv_date_udf("Ex-dividend date").alias("new_date")).show()
        
        import re
        def get_ticker_name(x):
            k = x[x.find("(")+1:x.rfind(")")]
            return k
        get_ticker_name_udf = udf(lambda j: get_ticker_name(j), StringType())
        
        df.select("company", get_ticker_name_udf("company").alias("company_sticker")).show()   
        
        strip_udf = udf(lambda j: j.strip(' %'), StringType())
    
        ###company,Ex-dividend date,Dividend,Type,Payment date,Yield
        
        bucket_prod_str = '/sr-investio-all-dwh/' + folder_name + '/'
        
        import pandas as pd
    
        df2 = df.select("company", get_ticker_name_udf("company").alias("company_sticker"), conv_date_udf("Ex-dividend date"),"Dividend","Type",conv_date_udf("Payment date"),"Yield" )
        
        df2.show()
        df2.printSchema()
        
        df2.toPandas().to_csv('s3://' + bucket_str + bucket_prod_str + new_file_name_short)
        
    print("\n done")