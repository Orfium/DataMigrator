# DataMigrator

 Using this Service, data from a Athena Table can be transfered to a table in Snowflake.
 
 An example usage would be:

 `python3 run.py -a "SELECT video_id,asset_id FROM reports.table" -s "DB_ORFIUM.SCHEMA.table"`
     
 Specifications:
      
 - The table in snowflake must already exists
 - The columns of the Athena must match exactly the columns of the Snowflake table
 - The snowflake user must be able to insert data data to the Snowflake table
 - The behavior of the service is append only
 
## To use the service the following Environmental Variables Must be Passed
 
### AWS CONFIGS

 - AWS_ACCESS_KEY
 - AWS_SECRET_KEY
 - AWS_REGION  
 - AWS_BUCKET_RESULTS  -  The S3 bucket that Athena service will put the data to
 - AWS_ATHENA_DB  -  The Database of Athena service that will pull data from


### SNOWFLAKE CONFIGS

- SNOWFLAKE_USER
- SNOWFLAKE_ROLE
- SNOWFLAKE_DB
- SNOWFLAKE_WH
- SNOWFLAKE_ACCOUNT
- SNOWFLAKE_PASSWORD
