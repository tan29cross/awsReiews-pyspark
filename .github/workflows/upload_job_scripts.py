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
    
  s3 =  session.client('s3')

  print('boto3 session created......')

  #file dir for pyspark scripts
  file_dir_py = os.path.join(FILE_PATH, 'Pyspark/')

  #file dir for state machine
  file_dir_sm = os.path.join(FILE_PATH, 'StateMachine/')

  #getting list of py scripts to upload to s3
  for file in os.walk(file_dir_py):
    if file.endswith('.py'):
        file_dir = os.path.join(FILE_PATH, file)
        print(f"Writing {file_dir} to bucket' ---> {BUCKET_NAME}")
   
       
        response = s3.upload_file(
                       Filename=file_dir_py,
                       Bucket=BUCKET_NAME,
                       Key='Scripts/{}'.format(file)
             )

        
    else:
        print('No file found with extension .py.........')


#uploading state machine template

  print("uploading state machine template to s3 asset bucket")

  response = s3.upload_file(
                       Filename=file_dir_sm,
                       Bucket=BUCKET_NAME,
                       Key='StateMachine/{}'.format(file)
             )


    


if __name__ == '__main__':
  main()