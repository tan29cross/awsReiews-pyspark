#importing required libraries
import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F,Window
from pyspark.sql.types import StructType, StructField, StringType,ArrayType
from pyspark.sql.utils import AnalysisException
import pandas as pd

#importing required aws libraries for aws glue job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from awsglue.context import GlueContext



#setting logging
import logging 

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())


if __name__ == "__main__":
 

    #getting job parameters that were defined in the job definition in the cloud formation template 
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'tempStorage', 'outputPath', 'rootTable', 's3SourcePath'] )

    #Declaring job parameters 
    glue_temp_storage = args['tempStorage']
    glue_output_s3_path = args['outputPath']
    glue_books_source_path = args['s3SourcePath']
    dfc_root_table_name = args['rootTable']

    # Create a SparkConf object
    conf = SparkConf().setAppName("amazon_reviews_part_2_2")

    ''''
    --> Used the config below only for local mode, just for demo here. Defined no of workers and worker type in the job parameter for the glue job

    conf.set("spark.executor.memory", "16")
    conf.set("spark.driver.memory", "8g")
    conf.set("spark.master", "local[4]")
    conf.set("spark.executor.instances", "4")
    conf.set('spark.executor.cores', '8')
    '''

    #Creating a spark context object using conf object ^
    sc = SparkContext(conf=conf)

    #Creating a spark session 
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.parquet.enableVectorizedReader","false")

    # create glue context with sc
    glueContext = GlueContext(sc)

    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    logger.info("Job Parameters Set, spark session and context created. Inititating job................")

    # Define the fields of the schema
    '''
    In the given schema, customer_id column is any given user_id,  where as  neighbouring_user_ids is an Array type column which contains all the users 
    who atleast bought one common product as the given user. This data is derived from the previous part i.e. 2.2.

    '''
    fields = [StructField("customer_id", StringType(), True),
              StructField("neighbouring_user_ids", ArrayType(StringType(), False), True)
         
          ]

    #creating a schema for atframes
    schema = StructType(fields)

    #reading data from s3 directory
    df_neighbours = spark.read.schema(schema).parquet(glue_books_source_path)


    #caching the source dataframe to be used later in other functions
    df_neighbours.cache()

    #creating a list of all user ids 
    all_user_ids = df_neighbours.rdd.map(lambda row: row.customer_id).collect()

    #creating an array of all user ids
    all_ids_array = F.array(*[F.lit(x) for x in all_user_ids])

    ''''
    In order to get an array of negative users, we will be using the array_except function here. The array_excpet function will take all_ids_array as first array 
    and neighbouring_user_ids column as second array. This operation will return an array that will contain all users which can be considered as negative users
    for  a given user
    '''

    result = df_neighbours.withColumn("negative_id", F.array_except(all_ids_array, F.col("neighbouring_user_ids")))

    result.cache() 

    #since the above operation results in 2 array columns; an explode operation is required on both the columns to get the data in desired format

    #exploding neighbouring_user_ids column
    result =  result.select(F.col("customer_id").alias('user_id'), F.explode("neighbouring_user_ids").alias("positive_id"), "negative_id")

    #exploding negative_id column
    result =  result.select('user_id', "positive_id", F.explode("negative_id").alias("negative_id") )

    logger.info("Started writing to s3 location")

    
    #writing data to s3 directory 
    result.write.mode("overwrite").parquet('glue_output_s3_path')

    logger.info("Finished writing to s3 location")