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

    ''''
    The following steps are involved in getting negative user_ids: 
    1. Create a self-join with df_neighbours where customer_ids don't match. 
    2. As a second step, it'd be required to filter out rows where the customer id from second dataframe is one of the neighbours of the customer id from the first datframe
    3. A udf will be used to filter out such rows i.e. if the customer id that didn't match during self-join but is also a neighbour of a given customer-id 
       will be filtered out. 
    '''

    #renaming columns to avoid duplication after self-join 
    df_neighbours_renamed = df_neighbours.select(F.col('customer_id').alias('user_id'), F.col('neighbouring_user_ids').alias('positive_user_id'))

    #creating a replica of the above df; to be used in self-join 
    df2 = df_neighbours_renamed.select(F.col('user_id').alias('neg_user_id'), F.col('positive_user_id').alias('negative_user_id'))

    #defining UDF to filter of neighbour user ids 
    check_user = F.udf(lambda x , y : x in y)

    
    # join the dataframe with itself to get all negative user ids
    df = df_neighbours_renamed.alias('df1').join(df2.alias('df2'), df_neighbours_renamed['user_id'] != df2['neg_user_id'],  'inner').filter(check_user("negative_user_id","positive_user_id") == False)

    #exploding postive user id column
    result = df.select('user_id', F.explode(F.col('positive_user_id')).alias(' positive_user_id'), F.col('neg_user_id').alias('negative_user_id') )

    result.cache() 


    logger.info("Started writing to s3 location")
    logger.info("Total records being written to s3")

    logger.info(result.count())

    
    #writing data to s3 directory 
    result.write.mode("overwrite").parquet(glue_output_s3_path)

    logger.info("Finished writing to s3 location")