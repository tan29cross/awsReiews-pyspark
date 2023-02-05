import os
import boto3
import botocore

def main():



  print("Starting to upload glue job to s3")
  AWS_ACCESS_ID = os.environ.get("AWS_ACCESS_ID")
  AWS_SECRET_KEY = os.environ.get("AWS_SECRET_KEY")
  AWS_REGION = os.environ.get("AWS_REGION")
  FILE_PATH = os.environ.get("FILE_PATH")
  BUCKET_NAME = os.environ.get("BUCKET_NAME")

  if not AWS_ACCESS_ID:
    raise RuntimeError("AWS_ACCESS_ID env var is not set!")

  elif not AWS_SECRET_KEY:
    raise RuntimeError("AWS_SECRET_KEY env var is not set!")
  
  else:
    print("All good! all env variables are set and good to go")

  #creating a boto3 session   
  session = boto3.Session (
    aws_access_key_id=AWS_ACCESS_ID,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name  = AWS_REGION)
    
  s3 = session.resource('s3')

  print('boto3 session created......')

  #getting list of py scripts to upload to s3
  for file in os.listdir(FILE_PATH):
      if file.endswith('.py'):
        print(f"Writing {file} to bucket' : {BUCKET_NAME}")
        try: 
            s3.Bucket(BUCKET_NAME).upload_file(file, "pyspark_scripts/{}".format(file))

        except botocore.exceptions.ClientError as error:
    
            raise error
      else:
        print('No file found with extension .py.........')


if __name__ == '__main__':
  main()