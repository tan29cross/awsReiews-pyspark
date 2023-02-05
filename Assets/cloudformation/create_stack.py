import os
import boto3
import botocore
from botocore.exceptions import ClientError


def main():



  print("Starting to upload glue job to s3")
  AWS_ACCESS_ID = os.environ.get("AWS_ACCESS_ID")
  AWS_SECRET_KEY = os.environ.get("AWS_SECRET_KEY")
  AWS_REGION = os.environ.get("AWS_REGION")
  FILE_PATH = os.environ.get("FILE_PATH")
  STACK_NAME = os.environ.get("STACK_NAME")

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
    
  cnfClient =  session.client('cloudformation')

  print('boto3 session created......')

  #getting list of py scripts to upload to s3
  

  #file_dir = os.path.join(FILE_PATH, file)

  #Declaring empty stack variable
  stack = ''

  with open(FILE_PATH, "r") as file:
        stack = file.read()
 
  #creating or updating stack 
  try:
     cnfClient.create_stack(StackName = STACK_NAME, TemplateBody = stack, Capabilities = ['CAPABILITY_NAMED_IAM', 'CAPABILITY_IAM']) 

  except botocore.exceptions.ClientError:
      cnfClient.update_stack(StackName = STACK_NAME, TemplateBody = stack, Capabilities = ['CAPABILITY_NAMED_IAM', 'CAPABILITY_IAM']) 

if __name__ == '__main__':
  main()