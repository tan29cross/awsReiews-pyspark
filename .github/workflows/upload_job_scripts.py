import os
import boto3

def main():



  print("Starting to upload glue job to s3")
  AWS_ACCESS_ID = os.environ.get("AWS_ACCESS_ID")
  AWS_SECRET_KEY = os.environ.get("AWS_SECRET_KEY")
  AWS_REGION = os.environ.get("AWS_REGION")
  FILE_PATH = os.environ.get("FILE_PATH")

  if not AWS_ACCESS_ID:
    raise RuntimeError("AWS_ACCESS_ID env var is not set!")

  elif not AWS_SECRET_KEY:
    raise RuntimeError("AWS_SECRET_KEY env var is not set!")
  
  else:
    print("All good! all env variables are set and good to go")

  
  #getting list of py scripts to upload to s3
  for file in os.listdir(FILE_PATH):
    print(file)
  

if __name__ == '__main__':
  main()