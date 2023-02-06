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
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'tempStorage', 'outputPath', 'rootTable', 's3BooksSourcePath', 's3MusicSourcePath', 'num_books_products', 'num_music_products', 'outputPath_stg'] )

    #Declaring job parameters 
    glue_temp_storage = args['tempStorage']
    glue_output_s3_path = args['outputPath']
    glue_output_s3_stg_path = args['outputPath_stg']
    glue_books_source_path = args['s3BooksSourcePath']
    glue_music_source_path = args['s3MusicSourcePath']
    dfc_root_table_name = args['rootTable']
    num_books_products =  args['num_books_products'] #variable to be used to get min num of books products
    num_music_products = args['num_music_products']  #variable to be used to get min num of music products

    

    # Create a SparkConf object
    conf = SparkConf().setAppName("amazon_reviews")

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

    
    # Define the fields of the schema; saving memory overhead in innfering schema at the time of reading parquet files from s3
    fields = [StructField("marketplace", StringType(), True),
          StructField("customer_id", StringType(), True),
          StructField("review_id", StringType(), True),
          StructField("product_id", StringType(), True),
          StructField("product_parent", StringType(), True),
          StructField("product_title", StringType(), True),
          StructField("star_rating", IntegerType(), True),
          StructField("helpful_votes", IntegerType(), True),
          StructField("total_votes", IntegerType(), True),
          StructField("vine", StringType(), True),
          StructField("verified_purchase", StringType(), True),
          StructField("review_headline", StringType(), True),
          StructField("review_body", StringType(), True),
          StructField("review_date", DateType(), True),
          StructField("year", IntegerType(), True)]
    
    #creating a schema for dataframes
    schema = StructType(fields)

    #reading books review data 
    df_books = spark.read.schema(schema).parquet(glue_books_source_path)

    #adding dataframe to cache to be re-used later and save re-computing of the dataframe
    df_books.cache()

    #Adding a suffix 'books' to books data to avoid duplication of column name when joined with music data 
    df_books_renamed = df_books.select([F.col(col_name).alias(col_name+'_books') for col_name in df_books.columns])

    logger.info("Reading book data finished. Initiating reading of music data........")

    #reading music reviews data 
    df_music = spark.read.schema(schema).parquet(glue_music_source_path)

    df_music.cache()

    #creating a constant column with product category == Music
    df_music =  df_music.withColumn('productCategory',F.lit("Music"))

    df_music_renamed = df_music.select([F.col(col_name).alias(col_name+'_music') for col_name in df_music.columns])

    #caching music data
    df_music_renamed.cache()

    #removing datframes from cache as no longer required 
    df_books.unpersist()
    df_music.unpersist()

    logger.info("Reading music data finished. Initiating join of music data and books data......")

    #selecting only the required columns for joins and pre-sorting on user_id (joining column) to avoid memory over heads
    df_books_renamed = df_books_renamed.select(F.col('customer_id_books'), F.col('product_id_books')).distinct().sort('customer_id_books')

    #Same for music df
    df_music_renamed = df_music_renamed.select(F.col('customer_id_music'), F.col('product_id_music')).distinct().sort('customer_id_music')

    '''
      Given, the size of the dataframes and limitations in number of executors & memory in local mode, I have decided to apply shuffle based join instead of a broadcast join which 
      will be more efficient with smaller datasets or with more memory.
    
    '''
    #joining the dataframes based on user id 
    df_common_users  = df_books_renamed.join(df_music_renamed, df_books_renamed['customer_id_books'] == df_music_renamed['customer_id_music'], how = 'inner' )
    
    #caching datframe for group by aggregation later
    df_common_users.cache()

    logger.info("Created a dataframe after joining music and books data. Now groupping joined data get min num of products bought......")

    #creating a dataframe for users who bought min number of books and music products as defined in job parameters above 
    df_common_users_grp = df_common_users.groupby('customer_id_books').agg(F.countDistinct('product_id_books').alias('num_books_products'), F.countDistinct('product_id_music').alias('num_music_products') )

    df_min_books_music = df_common_users_grp.filter(F.col('num_books_products')>num_books_products).filter(F.col('num_music_products') > num_music_products)

    logger.info('Initiating writing to s3.......')
    
    '''
    writing the dataframe to output location in a parquet format, reducing number of partitions on dataframe; at this stage data has 200 partitions, I just want to create 10 files when writing to s3 bucket; 
    avoids creating too many small files.
    
    '''
    #Using coalesce to avoid re-shuffle 
    df_min_books_music = df_min_books_music.coalesce(10)

    #writing to s3 
    df_min_books_music.write.mode("overwrite").parquet(glue_output_s3_path)


    #writing common users non-grouped data to a stage folder to be used a source for next part 
    df_common_users = df_common_users.coalesce(10)


     #writing ungrouped data to s3 
    df_common_users.write.mode("overwrite").parquet(glue_output_s3_stg_path)


    

