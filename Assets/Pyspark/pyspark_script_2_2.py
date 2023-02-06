#importing required libraries
import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F,Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType,DateType
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
    fields = [StructField("customer_id_books", StringType(), True),
              StructField("product_id_books", StringType(), True),
              StructField("customer_id_music", StringType(), True),
              StructField("product_id_music", StringType(), True),
             ]

    #creating a schema for atframes
    schema = StructType(fields)

    #reading data from s3 directory
    df_common_users = spark.read.schema(schema).parquet(glue_books_source_path)
    
    logger.info('total count....')
    logger.info(df_common_users.count())

    #caching the source dataframe to be used later in other functions
    df_common_users.cache()

    '''
    Splitting the dataframe into 2 halves; 1 containing only books products & music products. The idea here is to narrow down the datframe i.e just have 2 columns in the final dataframe 
    i.e. user and product id since we want to get a user's neighbour who atleast bought a common product. 

    Steps involved:

        1. Split dataframe into 2 halves i.e. books and music 
        2. Create a union of these datasets, so that we only have 2 columns i.e. customer_id & product_id 
        3. Do a self join on created dataframe in step 2 on product id to get users who have atleast one product in common
    
    '''

    #creating a df with books data 
    df_books_neighbour =  df_common_users.select(F.col('customer_id_books').alias('customer_id'), F.col('product_id_books').alias('product_id')).distinct().sort('product_id_books')
    df_books_neighbour.cache()

    #creating a df with music data 
    df_music_neighbour =  df_common_users.select(F.col('customer_id_music').alias('customer_id'), F.col('product_id_music').alias('product_id')).distinct().sort('product_id_music')
    df_music_neighbour.cache()

    #creating a union of above 2 df 
    df_music_books_neighbour = df_books_neighbour.union(df_music_neighbour)
    
    #caching df 
    df_music_books_neighbour.cache()
    
    logger.info('total count after union....')
    logger.info(df_music_books_neighbour.count())

    #removing from cache
    #df_music_neighbour.unpersist()
    #df_books_neighbour.unpersist()

    logger.info("Dataframe created with books and music data. Inititating self join................")
    
    #reducing the number of partitions from 200 to 48 reduce data shuffle 
    df_music_books_neighbour_repartitioned = df_music_books_neighbour.coalesce(50)
    df_music_books_neighbour_repartitioned.cache()

    #removing from cache
    df_music_books_neighbour.unpersist()

    #creating a replica of df for self join 
    df2=df_music_books_neighbour_repartitioned.select(F.col('customer_id').alias('neighbour_customer_id'),F.col('product_id').alias('neighbour_product_id') ).sort('neighbour_product_id')

    df2 = df2.coalesce(50)

    #creating a self-join 
    df_neighbour  = df_music_books_neighbour_repartitioned.alias('df1').join(df2, ((df_music_books_neighbour_repartitioned["product_id"] == df2["neighbour_product_id"]) & (df_music_books_neighbour["customer_id"] != df2["neighbour_customer_id"])), "inner" )
    
    # logging count of records to validate
    logger.info("Total records after self join")
    logger.info(df_neighbour.count())

    logger.info("Self-join operation completed. Inititating aggregate task................")
    
    
    #caching to be used later for aggregate operations
    df_neighbour.cache()

    df_neighbour_repartitioned = df_neighbour.coalesce(50)

    # using collect_list function to create a list of neighbour user ids 
    df_neighbour_final = df_neighbour_repartitioned.groupby('customer_id').agg(F.collect_list('neighbour_customer_id').alias('neighbouring_user_ids'))
    df_neighbour_final.cache()

    df_neighbour_final_repartitioned = df_neighbour_final.coalesce(50)
    df_neighbour_final.unpersist()


    logger.info("Started writing to s3 location")
    logger.info("Total number of records to write")
    logger.info(df_neighbour_final_repartitioned.count())

    
    #writing data to s3 directory 
    df_neighbour_final_repartitioned.write.mode("overwrite").parquet(glue_output_s3_path)

    logger.info("Finished writing to s3 location")









  