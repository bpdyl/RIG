import logging
import os
import sys

current_dir = os.getcwd()
log_file_path = os.path.join(current_dir, 'Logs')
if not os.path.exists(log_file_path):
  log_file_path = 'rig-executor/Logs'

print(log_file_path)

min_history_load_dt = '2020-02-02'
max_history_load_dt = '2020-08-26'


class CustomFormatter(logging.Formatter):

  def format(self, record):
    log_message = record.getMessage()
    dotted_line = '-' * 130  # Adjust the number of dashes as needed
    formatted_message = f'''
{dotted_line}
{log_message}
{dotted_line}
                '''
    return formatted_message


def execute_query(cursor, query, log_prefix, current_date, logger):
  attempt_date = current_date
  try:
    # Execute the query
    status = 'Started'
    cursor.execute(query)
    logger.info(f'''
{log_prefix} \non {attempt_date}: {status}
Snowflake Query ID : {cursor.sfqid}
Number of rows: {cursor.rowcount}
    ''')
    result = cursor.fetchall()
    return 'Success', result
  except Exception as e:
    status = f'Error - {str(e)}'
    logger.error(f'{log_prefix} on {attempt_date}: {status}')
    return status


def truncate_tables(cursor, tables, current_date, logger):
  for table in tables:
    query = f'TRUNCATE TABLE {table};'
    log_prefix = f'Attempt for truncate query {query} for fresh load'
    status = execute_query(cursor, query, log_prefix, current_date, logger)
    if 'Error' in status:
      sys.exit(status)
