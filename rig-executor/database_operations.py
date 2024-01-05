# database_operations.py

import snowflake.connector
import logging
import sys
from datetime import datetime


def establish_connection():
    # Set your Snowflake connection parameters
    account = 'robling_partner.east-us-2.azure'
    user = 'BPAUDYAL'
    password = 'bpSnowflake@123'
    role = 'ROBLING_DEVELOPER'
    warehouse = 'DEV_USER_WH'
    database = 'ROBLING_RIG_DB'

    conn = snowflake.connector.connect(user=user,
                                       password=password,
                                       account=account,
                                       role = role,
                                       warehouse=warehouse,
                                       database=database)
    return conn

def create_cursor(conn):
    return conn.cursor()

def close_connection(conn):
    conn.close()

def execute_query(cursor, query, log_prefix, current_date):
    attempt_date = current_date
    try:
        # Execute the query
        status = 'Started'
        logging.info(f'{log_prefix} on {attempt_date}: {status}')
        cursor.execute(query)
        return 'Success'
    except Exception as e:
        status = f'Error - {str(e)}'
        logging.error(f'{log_prefix} on {attempt_date}: {status}')
        return status

def truncate_tables(cursor, tables, current_date):
    for table in tables:
        query = f'TRUNCATE TABLE {table};'
        log_prefix = f'Attempt for truncate query {query} for fresh load'
        status = execute_query(cursor, query, log_prefix, current_date)
        if 'Error' in status:
            sys.exit(status)

def establish_and_execute(account, user, password, warehouse, database, query, log_prefix, current_date, tables=None):
    conn = establish_connection(account, user, password, warehouse, database)
    cursor = create_cursor(conn)

    if tables:
        truncate_tables(cursor, tables, current_date)

    status = execute_query(cursor, query, log_prefix, current_date)

    close_connection(conn)

    return status
