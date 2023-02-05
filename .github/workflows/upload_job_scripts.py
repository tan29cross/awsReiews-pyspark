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
  
  files = [os.path.join(dirpath, file) for (dirpath, dirnames, filenames) in os.walk(FILE_PATH) for file in filenames ]

  #getting list of py scripts to upload to s3
  for file in files:
    if file.endswith('.py'):
        file_dir = os.path.join(FILE_PATH, file)
        print(f"Writing {file_dir} to bucket' ---> {BUCKET_NAME}")
   
       
        response = s3.upload_file(
                       Filename=file,
                       Bucket=BUCKET_NAME,
                       Key='Scripts/{}'.format(file.split('/')[-1])
             )
    # state machine template
    elif file.endswith('.json'):
      response = s3.upload_file(
                       Filename=file,
                       Bucket=BUCKET_NAME,
                       Key='StateMachine/{}'.format(file.split('/')[-1])
             )

        
    else:
        print('No file found........directory does not contain suitable files to upload')



if __name__ == '__main__':
  main()