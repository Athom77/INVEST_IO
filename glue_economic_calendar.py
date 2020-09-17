###### INVESTIO glue job script - economic calendar

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
folder_name = 'economic-calendar'
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
            str_reform = datetime.strptime(x, '%A, %B %d, %Y')
            str_reform_2 = str_reform.strftime('%Y/%m/%d')
            return str_reform_2
        conv_date_udf = udf(lambda z: convert_date(z), StringType())

        df.select("date", conv_date_udf("date").alias("new_date")).show()
        
        import regex
        def conv_num(s):
            if s == "":
                return 0.0
            else:
                try:
                    return float(int(s))
                except:
                    return float(s)
                            
        def return_float(x):
            if x is not None:
                m = x.strip('/ -').replace(',','')
                k = regex.sub("[-+]?\d*\.\d+|\d+", "", m)
                j = conv_num(m.replace(k, ""))
                return j
            else:
                return ''
        
        return_float_udf = udf(lambda y: return_float(y), FloatType())
        
        def surprisef(x, y):
            try:
                k = str(return_float(x) / return_float(y) - 1.0)
            except:
                k = "N/A"
            return k
        surprise_udf = udf(lambda q, j: surprisef(q, j), StringType())
        
        def impo(x):
            imp_list = ['Holiday','Low Volatility Expected','Moderate Volatility Expected','High Volatility Expected']
            i = imp_list.index(x)
            return i
        imp_udf = udf(lambda m: impo(m), IntegerType())
        
        df.select("importance", imp_udf("importance").alias("new_imp")).show()

        def cutspaces(x):
            if x is not None:
                return x.strip('/ -')
            else:
                return x
        cutspaces_udf = udf(lambda x: cutspaces(x), StringType())
        ###date,time,currency,importance,event,actual,forecast,previousus
        
        bucket_prod_str = '/sr-investio-all-dwh/' + folder_name + '/'
    
        df2 = df.select("date", conv_date_udf("date").alias("new_date"), cutspaces_udf("currency").alias("currency formatted"), "importance",
        imp_udf("importance").alias("importance formatted"), "event", "actual", "forecast", "previous", return_float_udf("actual").alias("actual formatted"),
        return_float_udf("forecast").alias("forecast formatted"), return_float_udf("previous").alias("previous formatted"), surprise_udf("actual","forecast").alias("surprise"))
        
        df2.show()
        df2.printSchema()
        
        df2.toPandas().to_csv('s3://' + bucket_str + bucket_prod_str + new_file_name_short)
        
    print("\n done")