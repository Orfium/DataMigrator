import boto3
import time
from config import (
    AWS_BUCKET_RESULTS,
    AWS_ATHENA_DB,
    AWS_REGION,
    AWS_ACCESS_KEY,
    AWS_SECRET_KEY,
)

import logging

logger = logging.getLogger(__name__)


class QueryExecutionException(Exception):
    pass


class QueryExecutor(object):
    """
    Using the QueryExecutor, the query is performed using boto, hence
    the results are not tranferred into memory, instead the results are saved
    into S3. Once the execution finish, the location to S3 is returned as list
    of one. Use this Executor in queries for which the result set is large
    """

    def __init__(self):
        self.athena = boto3.client(
            "athena",
            region_name=AWS_REGION,
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
        )
        self.database = AWS_ATHENA_DB

    def _poll_status(self, _id, qry):
        time.sleep(5)
        while True:

            result = self.athena.get_query_execution(QueryExecutionId=_id)
            state = result["QueryExecution"]["Status"]["State"]
            if state == "SUCCEEDED":
                return "queries/" + _id + ".csv"
            elif state == "FAILED":
                raise QueryExecutionException(qry)
            else:
                # add some wait time before checking next time
                time.sleep(30)

    def execute(self, query_string: str) -> list:
        try:
            result = self.athena.start_query_execution(
                QueryString=query_string,
                QueryExecutionContext={"Database": self.database},
                ResultConfiguration={
                    "OutputLocation": f"s3://{AWS_BUCKET_RESULTS}/queries"
                },
            )
            execution_id = result["QueryExecutionId"]
            outputLocation = self._poll_status(_id=execution_id, qry=query_string)
            return [outputLocation]
        except Exception as e:
            logger.error(query_string)
            logger.error(e)
            raise QueryExecutionException(e)
