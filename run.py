import argparse

from src.migrator import process_transfer_athena_to_snowflake


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="""
        Using this tool data from a Athena Table can be tranfered to a table in Snowflake.
        Warnings:
        - The table in snowflake must already exists
        - The columns of the Athena must match the columns of Snowflake entire table 1-1
            """
    )

    # required arguments
    parser.add_argument(
        "-a",
        "--athena_qry",
        help="Query to Athena, SELECT a,b from reports.whatever",
        type=str,
        required=True,
    )
    parser.add_argument(
        "-s",
        "--snowflake_table",
        help="Snowflake Table, DB_ORFIUM.SCHEMA.T_WHATEVER",
        type=str,
        required=True,
    )

    args = parser.parse_args()
    return args


if __name__ == "__main__":
    arguments = parse_arguments()
    process_transfer_athena_to_snowflake(arguments.athena_qry, arguments.snowflake)
