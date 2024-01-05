from utils import truncate_tables,execute_query,CustomFormatter,log_file_path,max_history_load_dt,min_history_load_dt
import logging
from datetime import datetime,timedelta

# Set up logging for overstock
log_handler = logging.FileHandler(f'{log_file_path}/elgbl_and_l4w_sls.log', encoding='utf-8')
formatter = CustomFormatter()
log_handler.setFormatter(formatter)
logger = logging.getLogger('elgbl_and_l4w_sls')
logger.setLevel(logging.DEBUG)
logger.addHandler(log_handler)

def elgbl_mtx_load(cursor,attempt_date):
    print(f'I was called bro')
    truncate_tables(cursor,['DW_DWH.DWH_D_ELGBL_STY_COL_LOC_MTX'],attempt_date,logger)
    dwh_ins_query = '''
                INSERT INTO DW_DWH.DWH_D_ELGBL_STY_COL_LOC_MTX
                SELECT LOC.LOC_ID       AS LOC_ID
                    ,LOC.LOC_KEY        AS LOC_KEY
                    ,ITM.STY_ID         AS STY_ID
                    ,ITM.STY_KEY        AS STY_KEY
                    ,ITM.COLOR_ID       AS COLOR_ID
                    ,ITM.COLOR_KEY      AS COLOR_KEY
                    ,'Y'                AS INV_SLS_LY_FLG
                    ,CURRENT_TIMESTAMP  AS RCD_INS_TS
                    ,CURRENT_TIMESTAMP  AS RCD_UPD_TS
                FROM DW_DWH.DWH_F_SLS_TXN_LN_ITM_B SLS
                INNER JOIN DW_DWH.DWH_D_PRD_ITM_LU ITM ON ITM.ITM_KEY = SLS.ITM_KEY
                INNER JOIN DW_DWH.DWH_D_ORG_LOC_LU LOC ON LOC.LOC_KEY = SLS.LOC_KEY
                INNER JOIN DW_DWH.DWH_D_TIM_DAY_LU TIM ON TIM.DAY_KEY = SLS.TXN_DT_KEY
                INNER JOIN DW_DWH.DWH_D_CURR_TIM_LU CURR ON TIM.DAY_KEY >= to_date(to_char(dateadd(week, - 52, curr_day),'YYYY-MM-01'))
                GROUP BY 1,2,3,4,5,6
                HAVING SUM(F_SLS_QTY) > 0
                UNION
                SELECT LOC.LOC_ID       AS LOC_ID
                    ,LOC.LOC_KEY        AS LOC_KEY
                    ,ITM.STY_ID         AS STY_ID
                    ,ITM.STY_KEY        AS STY_KEY
                    ,ITM.COLOR_ID       AS COLOR_ID
                    ,ITM.COLOR_KEY      AS COLOR_KEY
                    ,'Y'                AS INV_SLS_LY_FLG
                    ,CURRENT_TIMESTAMP  AS RCD_INS_TS
                    ,CURRENT_TIMESTAMP  AS RCD_UPD_TS
                FROM DW_DWH.DWH_F_INV_ILD_B INV
                INNER JOIN DW_DWH.DWH_D_TIM_DAY_LU TIM ON TIM.DAY_KEY = INV.DAY_KEY
                INNER JOIN DW_DWH.DWH_D_PRD_ITM_LU ITM ON INV.ITM_KEY = ITM.ITM_KEY
                INNER JOIN DW_DWH.DWH_D_ORG_LOC_LU LOC ON INV.LOC_KEY = LOC.LOC_KEY
                INNER JOIN DW_DWH.DWH_D_CURR_TIM_LU CURR ON TIM.DAY_KEY >= to_date(to_char(dateadd(week, - 52, curr_day),'YYYY-MM-01'))
                GROUP BY 1,2,3,4,5,6
                HAVING SUM(F_OH_QTY) > 0;
                    '''
    dwh_ins_log_prefix = f"""Attempt for Eligiblity mtx table load: \n {dwh_ins_query}"""
    ddl_status,ins_result = execute_query(cursor, dwh_ins_query, dwh_ins_log_prefix,attempt_date,logger)
    print("I am here and i was successful")
    if ddl_status ==  'Success':
        logger.info(f'Eligibility table load successful. {ins_result}')
    else:
        logger.error(f'{ddl_status}')

def avg_lw_sls_hist_dwh(cursor,attempt_date):
    hist_tbl_ddl = ''' CREATE OR REPLACE TABLE DW_DWH.DWH_F_AVG_SLS_L4W_ILD_B_HIST CLONE DW_DWH.DWH_F_AVG_SLS_L4W_ILD_B;'''
    ddl_log_prefix = f"""Attempt for ddl of avg sls history table \n {hist_tbl_ddl}"""
    dwh_hist_ins_query = f'''
    INSERT INTO DW_DWH.DWH_F_AVG_SLS_L4W_ILD_B_HIST 
( DAY_KEY,
              ITM_ID,
              ITM_KEY,
              LOC_ID,
              LOC_KEY,
              AVG_L4W_SLS_QTY,
              AVG_L4W_SLS_RTL_LCL,
              AVG_L4W_SLS_CST_LCL,
              RCD_INS_TS,
              RCD_UPD_TS
              )
WITH WEEKLY_SLS_AGG AS 
(
  SELECT
  DISTINCT
    DATEADD(DAY, -28, DAY.DAY_KEY) AS END_DAY_KEY,
    DAY.DAY_KEY AS W_DAY_KEY
  FROM DW_DWH.DWH_F_SLS_TXN_LN_ITM_B SLS
  INNER JOIN DW_DWH.DWH_D_TIM_DAY_LU DAY
  ON SLS.TXN_DT_KEY = DAY.DAY_KEY
  WHERE DAY.DAY_KEY >= '{min_history_load_dt}' and DAY.DAY_KEY <='{max_history_load_dt}'
)

SELECT
  W.W_DAY_KEY AS DAY_KEY,
  ITM.ITM_ID AS ITM_ID,
  SLS.ITM_KEY AS ITM_KEY, 
  LOC.LOC_ID AS LOC_ID,
  SLS.LOC_KEY AS LOC_KEY,  
  SUM(F_SLS_QTY) / 28 AS AVG_L4W_SLS_QTY,
  SUM(F_SLS_RTL_LCL) / 28 AS AVG_L4W_SLS_RTL_LCL, 
  SUM(F_SLS_CST_LCL) / 28 AS AVG_L4W_SLS_CST_LCL,
  CURRENT_TIMESTAMP AS RCD_INS_TS,
  CURRENT_TIMESTAMP AS RCD_UPD_TS
FROM DW_DWH.DWH_F_SLS_TXN_LN_ITM_B SLS
INNER JOIN DW_DWH.DWH_D_PRD_ITM_LU ITM 
  ON SLS.ITM_KEY = ITM.ITM_KEY
INNER JOIN DW_DWH.DWH_D_ORG_LOC_LU LOC
  ON SLS.LOC_KEY = LOC.LOC_KEY   
INNER JOIN DW_DWH.DWH_D_TIM_DAY_LU DAY
  ON SLS.TXN_DT_KEY = DAY.DAY_KEY
INNER JOIN WEEKLY_SLS_AGG W
  ON DAY.DAY_KEY >= W.END_DAY_KEY AND 
     DAY.DAY_KEY < W.W_DAY_KEY
INNER JOIN DW_DWH.DWH_D_ELGBL_STY_COL_LOC_MTX ELGBL
  ON SLS.ITM_KEY = ITM.ITM_KEY AND SLS.LOC_KEY = ELGBL.LOC_KEY AND ITM.COLOR_KEY = ELGBL.COLOR_KEY AND ITM.STY_KEY = ELGBL.STY_KEY
GROUP BY 
  W.W_DAY_KEY,
  ITM.ITM_ID,
  SLS.ITM_KEY,
  LOC.LOC_ID, 
  SLS.LOC_KEY;
        '''
    hist_ins_log_prefix = f"""Attempt for Average sales L4W dwh history load: \n {dwh_hist_ins_query}"""
    ddl_status,ddl_result = execute_query(cursor, hist_tbl_ddl, ddl_log_prefix,attempt_date,logger)
    truncate_tables(cursor,['DW_DWH.DWH_F_AVG_SLS_L4W_ILD_B_HIST'],attempt_date,logger)
    hist_ins_status,hist_ins_result = execute_query(cursor, dwh_hist_ins_query, hist_ins_log_prefix,attempt_date,logger)
    if ddl_status == hist_ins_status == 'Success':
         logger.info(f'Avg Sls History dwh load Successful. {hist_ins_result}')
    else:
        logger.error(f'{ddl_status} and {hist_ins_status}')