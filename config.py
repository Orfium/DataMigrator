import os

# AWS CONFIGS
AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.environ.get("AWS_SECRET_KEY")
AWS_BUCKET_RESULTS = os.environ.get("AWS_BUCKET_RESULTS")
AWS_REGION = os.environ.get("AWS_REGION")
AWS_ATHENA_DB = os.environ.get("AWS_ATHENA_DB")


# SNOWFLAKE CONFIGS
SNOWFLAKE_USER = os.environ.get("SNOWFLAKE_USER")
SNOWFLAKE_ROLE = os.environ.get("SNOWFLAKE_ROLE")
SNOWFLAKE_DB = os.environ.get("SNOWFLAKE_DB")
SNOWFLAKE_WH = os.environ.get("SNOWFLAKE_WH")
SNOWFLAKE_ACCOUNT = os.environ.get("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_PASSWORD = os.environ.get("SNOWFLAKE_PASSWORD")
