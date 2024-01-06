from utils import truncate_tables, execute_query, CustomFormatter, log_file_path, max_history_load_dt, min_history_load_dt
import pytz
import logging
from datetime import datetime

tz = pytz.timezone('Asia/Kathmandu')
start_time = datetime.now(tz)


def sls_txn_quintiles_hist(cursor, attempt_date):
  # Set up logging for QUINTILES
  quintiles_handler = logging.FileHandler(f'{log_file_path}/txn_quintiles.log',
                                          encoding='utf-8')
  formatter = CustomFormatter()
  quintiles_handler.setFormatter(formatter)
  quintiles_logger = logging.getLogger('imminent_stockout')
  quintiles_logger.setLevel(logging.DEBUG)
  quintiles_logger.addHandler(quintiles_handler)
  # Insert into TMP table
  insert_tmp_query = f'''
      INSERT INTO DW_TMP.TMP_F_SLS_TXN_QUINTILES_B 
      (
          DAY_KEY,
          TXN_ID,
          RANK_BY_SLS,
          CNT_TXN_BY_DAY
      )
      SELECT
          DAY.DAY_KEY AS DAY_KEY,
          TXN_ID AS TXN_ID,
          RANK() OVER (
              PARTITION BY DAY.DAY_KEY 
              ORDER BY SUM(F_SLS_RTL_LCL) DESC
          ) AS RANK_BY_SLS,
          COUNT(TXN_ID) OVER(PARTITION BY DAY.DAY_KEY) AS CNT_TXN_BY_DAY
      FROM
          DW_DWH.DWH_F_SLS_TXN_LN_ITM_B TXN
          INNER JOIN DW_DWH.DWH_D_PRD_ITM_LU PRD ON TXN.ITM_KEY = PRD.ITM_KEY
          INNER JOIN DW_DWH.DWH_D_TIM_DAY_LU DAY ON TXN.DAY_KEY = DAY.DAY_KEY
      WHERE
          DAY.DAY_KEY >= '{min_history_load_dt}' and DAY.DAY_KEY < '{max_history_load_dt}'
      GROUP BY
          1, 2
      HAVING
          SUM(F_SLS_RTL_LCL) > 0;
  '''

  # Insert into DWH table
  insert_dwh_query = '''
      INSERT INTO DW_DWH.DWH_F_SLS_TXN_QUINTILES_B
      (
          DAY_KEY,
          TXN_ID,
          QTILE_SLS,
          CNT_BY_TXN
      )
      SELECT
          DAY.DAY_KEY AS DAY_KEY,
          TMP.TXN_ID AS TXN_ID,
          RANK_BY_SLS / CNT_TXN_BY_DAY AS QTILE_SLS,
          COUNT(DISTINCT TMP.TXN_ID) AS CNT_BY_TXN
      FROM
          DW_TMP.TMP_F_SLS_TXN_QUINTILES_B TMP
          INNER JOIN DW_DWH.DWH_D_TIM_DAY_LU DAY ON COALESCE(TMP.DAY_KEY, DAY.DAY_KEY) = DAY.DAY_KEY
      GROUP BY
          1, 2, 3;
  '''

  insert_tmp_log_prefix = f"Attempt for inserting into TMP table \n {insert_tmp_query}"
  insert_dwh_log_prefix = f"Attempt for inserting into DWH table \n {insert_dwh_query}"

  # Execute queries
  insert_tmp_status, insert_tmp_result = execute_query(cursor,
                                                       insert_tmp_query,
                                                       insert_tmp_log_prefix,
                                                       attempt_date,
                                                       quintiles_logger)
  insert_dwh_status, insert_dwh_result = execute_query(cursor,
                                                       insert_dwh_query,
                                                       insert_dwh_log_prefix,
                                                       attempt_date,
                                                       quintiles_logger)

  # Check status and log
  if insert_tmp_status == insert_dwh_status == 'Success':
    quintiles_logger.info(f'''
          Sales Transaction Quintiles data load Successful.
          Insert into TMP Query Result: {insert_tmp_result}
          Insert into DWH Query Result: {insert_dwh_result}
      ''')
  else:
    quintiles_logger.error(f'{insert_tmp_status} and {insert_dwh_status}')
