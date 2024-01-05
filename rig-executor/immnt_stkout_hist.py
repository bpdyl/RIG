from utils import truncate_tables,execute_query,CustomFormatter,log_file_path,max_history_load_dt,min_history_load_dt
import pytz
import logging
from datetime import datetime

tz = pytz.timezone('Asia/Kathmandu')
start_time = datetime.now(tz)   

# Set up logging for imminent stockout
imminent_stockout_handler = logging.FileHandler(f'{log_file_path}/imminent_stkout_hist.log', encoding='utf-8')
formatter = CustomFormatter()
imminent_stockout_handler.setFormatter(formatter)
imminent_stockout_logger = logging.getLogger('imminent_stockout')
imminent_stockout_logger.setLevel(logging.DEBUG)
imminent_stockout_logger.addHandler(imminent_stockout_handler)

def immnt_stkout_hist_dwh(cursor,attempt_date):
     hist_tbl_ddl = ''' CREATE OR REPLACE TABLE DW_DWH.DWH_F_INV_IMMNT_STKOUT_ILD_B_HIST CLONE DW_DWH.DWH_F_INV_IMMNT_STKOUT_ILD_B; '''
     ddl_log_prefix = f"""Attempt for ddl of immnt_stkout history table \n {hist_tbl_ddl}"""
     dwh_hist_ins_query = '''
        INSERT INTO DW_DWH.DWH_F_INV_IMMNT_STKOUT_ILD_B_HIST
            (
            DAY_KEY
            ,ITM_ID
            ,ITM_KEY
            ,LOC_ID
            ,LOC_KEY
            ,LCL_CNCY_CDE
            ,COUNT_IMMIN_STOCKOUT
            ,AVG_L4W_SLS_QTY
            ,AVG_L4W_SLS_RTL
            ,AVG_L4W_SLS_CST
            ,IMMIN_STOCKOUT_THRESHOLD
            ,NUM_DAYS_OF_SUPPLY
            ,RCD_INS_TS
            ,RCD_UPD_TS
            )
SELECT SLS.DAY_KEY                                            AS DAY_KEY
      ,PRD.ITM_ID                                             AS ITM_ID
      ,SLS.ITM_KEY                                            AS ITM_KEY
      ,LOC.LOC_ID                                             AS LOC_ID
      ,SLS.LOC_KEY                                            AS LOC_KEY
      ,INV.LCL_CNCY_CDE                                       AS LCL_CNCY_CDE
      ,1                                                      AS COUNT_IMMIN_STOCKOUT
      ,SUM(AVG_L4W_SLS_QTY)                                   AS AVG_L4W_SLS_QTY
      ,SUM(AVG_L4W_SLS_RTL_LCL)                               AS AVG_L4W_SLS_RTL
      ,SUM(AVG_L4W_SLS_CST_LCL)                               AS AVG_L4W_SLS_CST
      ,14                                                     AS IMMIN_STOCKOUT_THRESHOLD
      ,SUM((INV.F_OH_QTY+INV.F_IT_QTY)/SLS.AVG_L4W_SLS_QTY)   AS NUM_DAYS_OF_SUPPLY
      ,CURRENT_TIMESTAMP                                      AS RCD_INS_TS
      ,CURRENT_TIMESTAMP                                      AS RCD_UPD_TS
FROM  DW_DWH.DWH_F_INV_ILD_B INV
INNER JOIN DW_DWH.DWH_F_AVG_SLS_L4W_ILD_B_HIST SLS ON SLS.ITM_KEY=INV.ITM_KEY AND SLS.LOC_KEY=INV.LOC_KEY AND (SLS.DAY_KEY BETWEEN INV.DAY_KEY AND INV.END_DAY_KEY)
INNER JOIN DW_DWH.DWH_D_PRD_ITM_LU PRD ON INV.ITM_KEY = PRD.ITM_KEY
INNER JOIN DW_DWH.DWH_D_ORG_LOC_LU LOC ON INV.LOC_KEY = LOC.LOC_KEY
WHERE INV.F_OH_QTY > 0 AND SLS.AVG_L4W_SLS_QTY > 0
GROUP BY 1,2,3,4,5,6
HAVING NUM_DAYS_OF_SUPPLY  <  IMMIN_STOCKOUT_THRESHOLD
;
          '''
     hist_ins_log_prefix = f"""Attempt for imminent stockout dwh history load:  \n {dwh_hist_ins_query}"""
     ddl_status,ddl_result = execute_query(cursor, hist_tbl_ddl, ddl_log_prefix,attempt_date,imminent_stockout_logger)
     truncate_tables(cursor,['DW_DWH.DWH_F_INV_IMMNT_STKOUT_ILD_B_HIST'],attempt_date,imminent_stockout_logger)
     hist_ins_status,hist_ins_result = execute_query(cursor, dwh_hist_ins_query, hist_ins_log_prefix,attempt_date,imminent_stockout_logger)
     if ddl_status == hist_ins_status == 'Success':
        imminent_stockout_logger.info(f'Immnt Stockout History dwh load Successful. {hist_ins_result}')
     else:
        imminent_stockout_logger.error(f'{ddl_status} and {hist_ins_status}')

def immnt_stkout_hist_dm(cursor,attempt_date):
    delete_query = f'''DELETE FROM DM_MERCH.DM_F_MEAS_FACT_ILD_B WHERE FACT_CDE = 1700;--imminent stockout '''
    dm_ins_query = f'''
                INSERT INTO DM_MERCH.DM_F_MEAS_FACT_ILD_B 
                (
                MEAS_DT
                ,ITM_KEY
                ,LOC_KEY
                ,DIV_KEY
                ,CHN_KEY
                ,CHNL_ID
                ,FACT_CDE
                ,LCL_CNCY_CDE
                ,F_FACT_COL1
                ,F_FACT_COL2
                ,F_FACT_COL3
                ,F_FACT_COL4
                ,F_FACT_COL5
                ,F_FACT_COL6
                )
            SELECT SRC.DAY_KEY                      AS MEAS_DT
                ,SRC.ITM_KEY                        AS ITM_KEY
                ,SRC.LOC_KEY                        AS LOC_KEY
                ,ITM.DIV_KEY                        AS DIV_KEY
                ,LOC.CHN_KEY                        AS CHN_KEY
                ,LOC.CHNL_ID                        AS CHNL_ID
                ,1700                               AS FACT_CDE
                ,SRC.LCL_CNCY_CDE                   AS LCL_CNCY_CDE
                ,SRC.COUNT_IMMIN_STOCKOUT           AS F_FACT_COL1
                ,SRC.AVG_L4W_SLS_QTY                AS F_FACT_COL2
                ,SRC.AVG_L4W_SLS_RTL                AS F_FACT_COL3	
                ,SRC.AVG_L4W_SLS_CST                AS F_FACT_COL4 
                ,SRC.IMMIN_STOCKOUT_THRESHOLD       AS F_FACT_COL5
                ,SRC.NUM_DAYS_OF_SUPPLY             AS F_FACT_COL6
            FROM DW_DWH.DWH_F_INV_IMMNT_STKOUT_ILD_B_HIST SRC
            INNER JOIN DW_DWH.DWH_D_PRD_ITM_LU ITM ON ITM.ITM_KEY = SRC.ITM_KEY
            INNER JOIN DW_DWH.DWH_D_ORG_LOC_LU LOC ON LOC.LOC_KEY = SRC.LOC_KEY
            --WHERE DAY_KEY <> '2023-12-13';
        '''
    delete_query_log_prefix = f"""Attempt for datamart delete query \n {delete_query}"""
    dm_ins_query_log_prefix = f"""Attempt for datamart insert query \n {dm_ins_query}"""
    delete_query_status,del_query_result = execute_query(cursor, delete_query, delete_query_log_prefix,attempt_date,imminent_stockout_logger)
    dm_ins_query_status,dm_ins_result = execute_query(cursor, dm_ins_query, dm_ins_query_log_prefix,attempt_date,imminent_stockout_logger)
    if delete_query_status == dm_ins_query_status == 'Success':
         imminent_stockout_logger.info(f'''Immnt Stockout History datamart load Successful. 
                                       Del query Result: {del_query_result} \
                                       DM insert query Result: {dm_ins_result}
                                       ''')
    else:
        imminent_stockout_logger.error(f'{delete_query_status} and {dm_ins_query_status}')

