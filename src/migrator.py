import logging
import uuid
from src.athena import QueryExecutor
from src.s3_connector import S3Executor
from src.snowflake import SnowFlakeExecutor

logger = logging.getLogger(__name__)


def process_transfer_athena_to_snowflake(athena_qry, snowflake_table):

    # execute in athena and get its S3 location
    db = QueryExecutor()
    data = db.execute(athena_qry)
    s3_path = data[0]
    s3 = S3Executor()

    # save to random file under temp to avoid conflicts
    filepath = "/tmp/" + str(uuid.uuid4())[0:5] + ".csv"
    s3.download_file(s3_path, filepath)

    # create a random staging name to avoid conflicts
    staging_temp_name = "my_stage_" + str(uuid.uuid4())[0:5]

    # create temp stage
    qry_start = f"CREATE TEMPORARY STAGE {staging_temp_name}"

    # transfer athena results to the temp stage
    qry_put_data = f"PUT file://{filepath} @{staging_temp_name}"

    # insert data from staging to snowflake
    qry_insert = f"copy into {snowflake_table} from @{staging_temp_name} file_format = (type = csv FIELD_OPTIONALLY_ENCLOSED_BY = '\"' skip_header = 1);"

    # drop temp staging
    qry_end = f"DROP STAGE {staging_temp_name}"

    # Perform the sequence of sql queries
    with SnowFlakeExecutor() as db:

        logger.info(qry_start + "...")
        db.execute_query(qry_start)

        logger.info(qry_put_data + "...")
        db.execute_query(qry_put_data)

        logger.info(qry_insert + "...")
        db.execute_query(qry_insert)

        logger.info(qry_end + "...")
        db.execute_query(qry_end)

        logger.info("Success...")
