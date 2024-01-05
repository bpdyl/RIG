from utils import truncate_tables,execute_query,CustomFormatter,log_file_path,max_history_load_dt,min_history_load_dt
import pytz
import logging
from datetime import datetime

tz = pytz.timezone('Asia/Kathmandu')
start_time = datetime.now(tz)   

# Set up logging for stockout
stockout_handler = logging.FileHandler(f'{log_file_path}/stockout_hist.log', encoding='utf-8')
formatter = CustomFormatter()
stockout_handler.setFormatter(formatter)
stockout_logger = logging.getLogger('stockout')
stockout_logger.setLevel(logging.DEBUG)
stockout_logger.addHandler(stockout_handler)


def stkout_hist_dwh(cursor,attempt_date):
     hist_tbl_ddl = ''' CREATE OR REPLACE TABLE DW_DWH.DWH_F_INV_STKOUT_ILD_B_HIST CLONE DW_DWH.DWH_F_INV_STKOUT_ILD_B; '''
     ddl_log_prefix = f"""Attempt for ddl of stkout history table \n {hist_tbl_ddl}"""
     dwh_hist_ins_query = '''
        INSERT INTO DW_DWH.DWH_F_INV_STKOUT_ILD_B_HIST (
        DAY_KEY
        ,ITM_ID
        ,ITM_KEY
        ,LOC_ID
        ,LOC_KEY
        ,LCL_CNCY_CDE
        ,COUNT_STOCKOUT
        ,COUNT_DAYS
        ,COUNT_WEEKEND_DAYS
        ,AVG_L4W_SLS_QTY
        ,AVG_L4W_SLS_RTL
        ,AVG_L4W_SLS_CST
        ,RCD_INS_TS
        ,RCD_UPD_TS
)
SELECT  SLS.DAY_KEY                                                                                       AS DAY_KEY
       ,ITM.ITM_ID                                                                                        AS ITM_ID
       ,SLS.ITM_KEY                                                                                       AS ITM_KEY
       ,LOC.LOC_ID                                                                                        AS LOC_ID
       ,SLS.LOC_KEY                                                                                       AS LOC_KEY
       ,INV.LCL_CNCY_CDE                                                                                  AS LCL_CNCY_CDE
       ,1                                                                                                 AS COUNT_STOCKOUT
       ,1                                                                                                 AS COUNT_DAYS
      ,CASE

          WHEN MAX(DAY.WK_DAY_DESC) = 'Sunday' OR  MAX(DAY.WK_DAY_DESC) = 'Saturday'
                  THEN 1
          ELSE 0
        END                                                                                               AS COUNT_WEEKEND_DAYS
       ,SUM(AVG_L4W_SLS_QTY)                                                                              AS AVG_L4W_SLS_QTY
       ,SUM(SLS.AVG_L4W_SLS_RTL_LCL)                                                                      AS AVG_L4W_SLS_RTL
       ,SUM(SLS.AVG_L4W_SLS_CST_LCL)                                                                      AS AVG_L4W_SLS_CST
       ,CURRENT_TIMESTAMP                                                                                 AS RCD_INS_TS
       ,CURRENT_TIMESTAMP                                                                                 AS RCD_UPD_TS
FROM DW_DWH.DWH_F_INV_ILD_B INV
INNER JOIN DW_DWH.DWH_F_AVG_SLS_L4W_ILD_B_HIST SLS
ON SLS.ITM_KEY = INV.ITM_KEY AND SLS.LOC_KEY = INV.LOC_KEY AND SLS.DAY_KEY BETWEEN INV.DAY_KEY AND INV.END_DAY_KEY
INNER JOIN DW_DWH.DWH_D_ORG_LOC_LU LOC ON INV.LOC_KEY = LOC.LOC_KEY 
INNER JOIN DW_DWH.DWH_D_PRD_ITM_LU ITM ON INV.ITM_KEY = ITM.ITM_KEY
INNER JOIN DW_DWH.DWH_D_TIM_DAY_LU DAY
ON DAY.DAY_KEY = SLS.DAY_KEY
GROUP BY  1
         ,2
         ,3
         ,4
         ,5
         ,6
         ,7
         ,8
HAVING SUM(F_OH_QTY) <= 0 AND SUM(AVG_L4W_SLS_QTY) > 0;
          '''
     hist_ins_log_prefix = f"""Attempt for stockout dwh history load: \n {dwh_hist_ins_query}"""
     ddl_status,ddl_result = execute_query(cursor, hist_tbl_ddl, ddl_log_prefix,attempt_date,stockout_logger)
     truncate_tables(cursor,['DW_DWH.DWH_F_INV_STKOUT_ILD_B_HIST'],attempt_date,stockout_logger)
     hist_ins_status,hist_ins_result = execute_query(cursor, dwh_hist_ins_query, hist_ins_log_prefix,attempt_date,stockout_logger)
     if ddl_status == hist_ins_status == 'Success':
        stockout_logger.info(f'''Stockout History dwh load Successful. {hist_ins_result}''')
     else:
        stockout_logger.error(f'{ddl_status} and {hist_ins_status}')

def stkout_hist_dm(cursor,attempt_date):
    delete_query = f'''DELETE FROM DM_MERCH.DM_F_MEAS_FACT_ILD_B WHERE FACT_CDE = 1500;-- stockout '''
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
SELECT           SRC.DAY_KEY                    AS MEAS_DT
                ,SRC.ITM_KEY                    AS ITM_KEY
                ,SRC.LOC_KEY                    AS LOC_KEY
                ,ITM.DIV_KEY                    AS DIV_KEY
                ,LOC.CHN_KEY                    AS CHN_KEY
                ,LOC.CHNL_ID                    AS CHNL_ID
                ,1500                           AS FACT_CDE
                ,SRC.LCL_CNCY_CDE               AS LCL_CNCY_CDE
                ,SRC.COUNT_STOCKOUT             AS F_FACT_COL1
                ,SRC.COUNT_DAYS                 AS F_FACT_COL2
                ,SRC.COUNT_WEEKEND_DAYS         AS F_FACT_COL3
                ,SRC.AVG_L4W_SLS_QTY            AS F_FACT_COL4
                ,SRC.AVG_L4W_SLS_RTL            AS F_FACT_COL5	
                ,SRC.AVG_L4W_SLS_CST            AS F_FACT_COL6
            FROM DW_DWH.DWH_F_INV_STKOUT_ILD_B_HIST SRC
            INNER JOIN DW_DWH.DWH_D_PRD_ITM_LU ITM ON ITM.ITM_KEY = SRC.ITM_KEY
            INNER JOIN DW_DWH.DWH_D_ORG_LOC_LU LOC ON LOC.LOC_KEY = SRC.LOC_KEY
            --WHERE SRC.DAY_KEY <> '2023-12-13'
            ;
        '''
    delete_query_log_prefix = f"""Attempt for datamart delete query \n {delete_query}"""
    dm_ins_query_log_prefix = f"""Attempt for datamart insert query \n {dm_ins_query}"""
    delete_query_status,delete_query_result = execute_query(cursor, delete_query, delete_query_log_prefix,attempt_date,stockout_logger)
    dm_ins_query_status,dm_ins_query_result = execute_query(cursor, dm_ins_query, dm_ins_query_log_prefix,attempt_date,stockout_logger)
    if delete_query_status == dm_ins_query_status == 'Success':
        stockout_logger.info(f'''Stockout History datamart load Successful. 
                             Del query Result: {delete_query_result} \
                             Dm Insert Query Result : {dm_ins_query_result}
                             ''')
    else:
        stockout_logger.error(f'{delete_query_status} and {dm_ins_query_status}')

