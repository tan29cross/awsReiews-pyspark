# awsReiews-pyspark
This repository contains artifacts to set up a serverless AWS env and run etl jobs on amazon reviews data set. This project makes use of AWS services such as s3, IAM, Cloudformation, Step Function and AWS glue. In addition, there is a github action which deploys all the artifacts in the AWS env. 

# Contents

- EDA_AWS_Reviews.ipynb - Jupyter notebook containing exploratory data analysis.

- Assets - Contains all the project assests such as:
   1. Pyspark ETL scripts
   2. Cloudformation templates for IO, Glue jobs and Step function (state machine)

- .github/workflows - Contains artifiacts for a git hub action that sets up the AWS env and performs the following:
   1. Create stacks for I/O, Glue jobs,Step Function.
   2. Creates service roles in IAM to run Glue Jobs and trigger step function 
   3. Uploads scripts to s3 bucket for Glue Jobs
   
# Setup Guide

1. Clone this repository on a local disk 

2. Change <b>AWS_ACCOUNT_ID</b> in the parameters section all cloud formation template files including <b>cf_template_Stackpart21.json</b>, <b>cf_template_Stackpart22.json</b>, <b>cf_template_Stackpart23.json</b>, <b>cf_template_state_machine.json</b>. Example shown below: 

![image](https://user-images.githubusercontent.com/29129015/216891996-9cdde440-8fd0-4be1-9599-a604c1447dbb.png)


3. Once a new repository is created, add/update git hub secret and add your <b>AWS_ACCESS_ID</b>, <b>AWS_SECRET_KEY</b> as git hub secrets to run the wrkflow.

4. Run the workflow 'Set up AWS env' git hub action. This will trigger the creation all the artifacts within the aws env. 

5. Once the git hub action is completed, go to step function in your AWS account to manually trigger the step function to run the ETL jobs in sequence. Here what the step functions looks like: 

![image](https://user-images.githubusercontent.com/29129015/216908457-130a0c3a-9d26-4adb-96d6-9dc7dc56d8b3.png)

