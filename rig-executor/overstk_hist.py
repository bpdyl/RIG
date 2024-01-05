from utils import truncate_tables,execute_query,CustomFormatter,log_file_path,max_history_load_dt,min_history_load_dt
import pytz
from datetime import datetime,timedelta
from tqdm import tqdm
import sys
import logging

tz = pytz.timezone('Asia/Kathmandu')
start_time = datetime.now(tz)   

# Set up logging for overstock
overstk_handler = logging.FileHandler(f'{log_file_path}/overstock_hist.log', encoding='utf-8')
formatter = CustomFormatter()
overstk_handler.setFormatter(formatter)
overstk_logger = logging.getLogger('overstock')
overstk_logger.setLevel(logging.DEBUG)
overstk_logger.addHandler(overstk_handler)


def overstk_hist_dwh(cursor,attempt_date):
     hist_tbl_ddl = ''' CREATE OR REPLACE TABLE DW_DWH.DWH_F_INV_OVERSTK_ILD_B_HIST CLONE DW_DWH.DWH_F_INV_OVERSTK_ILD_B; '''
     ddl_log_prefix = f"""Attempt for ddl of overstk history table \n {hist_tbl_ddl}"""
     dwh_hist_ins_query = f'''
        INSERT INTO DW_DWH.DWH_F_INV_OVERSTK_ILD_B_HIST(
                            DAY_KEY
                            ,LOC_ID
                            ,LOC_KEY
                            ,ITM_ID
                            ,ITM_KEY
                            ,LCL_CNCY_CDE
                            ,COUNT_OVERSTOCK
                            ,OVERSTOCK_DAY_THRESHOLD
                            ,NUM_DAYS_OF_SUPPLY
                            ,AVG_L4W_SLS_RTL
                            ,AVG_L4W_SLS_QTY
                            ,AVG_L4W_SLS_CST
                            ,OVERSTOCK_INV_RTL
                            ,OVERSTOCK_INV_QTY
                            ,OVERSTOCK_INV_CST
                            ,DAYS_SINCE_SALE
                            ,RCD_INS_TS
                            ,RCD_UPD_TS
                            )
(

SELECT  DAY_KEY
       ,LOC_ID
       ,LOC_KEY
       ,ITM_ID
       ,ITM_KEY
       ,LCL_CNCY_CDE
       ,COUNT_OVERSTOCK
       ,OVERSTOCK_DAY_THRESHOLD
       ,NUM_DAYS_OF_SUPPLY
       ,L4W_SLS_RTL
       ,L4W_SLS_QTY
       ,L4W_SLS_CST
       ,OVERSTOCK_INV_RTL
       ,OVERSTOCK_INV_QTY
       ,OVERSTOCK_INV_CST
       ,DAYS_SINCE_SALE
       ,CURRENT_TIMESTAMP
       ,CURRENT_TIMESTAMP
FROM
(
	SELECT  AVG_SLS.DAY_KEY                                                                                         AS DAY_KEY
	       ,LOC.LOC_ID                                                                                              AS LOC_ID
	       ,INV.LOC_KEY                                                                                             AS LOC_KEY
	       ,ITM.ITM_ID                                                                                              AS ITM_ID
	       ,INV.ITM_KEY                                                                                             AS ITM_KEY
	       ,INV.LCL_CNCY_CDE                                                                                        AS LCL_CNCY_CDE
	       ,42                                                                                                      AS OVERSTOCK_DAY_THRESHOLD
	       ,SUM(AVG_L4W_SLS_RTL_LCL)                                                                                AS L4W_SLS_RTL
	       ,SUM(AVG_L4W_SLS_QTY)                                                                                    AS L4W_SLS_QTY
	       ,SUM(AVG_L4W_SLS_CST_LCL)                                                                                AS L4W_SLS_CST
	       ,SUM(F_OH_QTY)                                                                                           AS INV_QTY
	       ,SUM(F_OH_RTL_LCL)                                                                                       AS INV_RTL
	       ,SUM(F_OH_CST_LCL)                                                                                       AS INV_CST
	       ,INV_QTY / NULLIF(L4W_SLS_QTY,0)                                                                         AS NUM_DAYS_OF_SUPPLY
	       ,1                                                                                                       AS COUNT_OVERSTOCK
	       ,CASE WHEN NUM_DAYS_OF_SUPPLY IS NULL THEN INV_QTY  ELSE (INV_QTY - L4W_SLS_QTY * 42) END                AS OVERSTOCK_INV_QTY
	       ,CASE WHEN NUM_DAYS_OF_SUPPLY IS NULL THEN INV_RTL  ELSE OVERSTOCK_INV_QTY * SUM(F_UNIT_RTL_LCL) END     AS OVERSTOCK_INV_RTL
	       ,CASE WHEN NUM_DAYS_OF_SUPPLY IS NULL THEN INV_CST  ELSE OVERSTOCK_INV_QTY * SUM(F_UNIT_WAC_CST_LCL) END AS OVERSTOCK_INV_CST
	       ,DATEDIFF(DAY,(
                  SELECT  MAX(TXN_DT_KEY)
                  FROM DW_DWH.DWH_F_SLS_TXN_LN_ITM_B SLS
                  WHERE SLS.ITM_KEY = AVG_SLS.ITM_KEY
                  AND SLS.LOC_KEY = AVG_SLS.LOC_KEY
                  AND TXN_DT_KEY <= AVG_SLS.DAY_KEY), AVG_SLS.DAY_KEY)                                             AS DAYS_SINCE_SALE
	FROM DW_DWH.DWH_F_INV_ILD_B INV
	LEFT OUTER JOIN DW_DWH.DWH_F_AVG_SLS_L4W_ILD_B_HIST AVG_SLS
	ON INV.ITM_KEY = AVG_SLS.ITM_KEY AND INV.LOC_KEY = AVG_SLS.LOC_KEY AND (AVG_SLS.DAY_KEY BETWEEN INV.DAY_KEY AND INV.END_DAY_KEY)
    INNER JOIN DW_DWH.DWH_D_TIM_DAY_LU TIM ON TIM.DAY_KEY = AVG_SLS.DAY_KEY
	INNER JOIN DW_DWH.DWH_D_PRD_ITM_LU ITM
	ON INV.ITM_KEY = ITM.ITM_KEY
	INNER JOIN DW_DWH.DWH_D_ORG_LOC_LU LOC
	ON INV.LOC_KEY = LOC.LOC_KEY
	WHERE LOC.CHNL_DESC <> 'eCommerce' AND TIM.DAY_KEY>='{min_history_load_dt}'
	GROUP BY  ALL
	HAVING SUM(F_OH_QTY) > 0 AND (NUM_DAYS_OF_SUPPLY IS NULL OR NUM_DAYS_OF_SUPPLY > OVERSTOCK_DAY_THRESHOLD)
)
);
          '''
     hist_ins_log_prefix = f"""Attempt for Overstock dwh history load: \n {dwh_hist_ins_query}"""
     ddl_status,ddl_result = execute_query(cursor, hist_tbl_ddl, ddl_log_prefix,attempt_date,overstk_logger)
     truncate_tables(cursor,['DW_DWH.DWH_F_INV_OVERSTK_ILD_B_HIST'],attempt_date,overstk_logger)
     hist_ins_status,ins_result = execute_query(cursor, dwh_hist_ins_query, hist_ins_log_prefix,attempt_date,overstk_logger)
     if ddl_status == hist_ins_status == 'Success':
           overstk_logger.info(f'Overstock History dwh load Successful. {ins_result}')
     else:
          overstk_logger.error(f'{ddl_status} and {hist_ins_status}')

def overstk_hist_dm(cursor,attempt_date):
    delete_query = f'''DELETE FROM DM_MERCH.DM_F_MEAS_FACT_ILD_B WHERE FACT_CDE = 1600;-- Overstock '''
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
                SELECT SRC.DAY_KEY               AS MEAS_DT   
                ,SRC.ITM_KEY                     AS ITM_KEY   
                ,SRC.LOC_KEY                     AS LOC_KEY   
                ,ITM.DIV_KEY                     AS DIV_KEY   
                ,LOC.CHN_KEY                     AS CHN_KEY   
                ,LOC.CHNL_ID                     AS CHNL_ID   
                ,1600                            AS FACT_CDE   
                ,SRC.LCL_CNCY_CDE                AS LCL_CNCY_CDE   
                ,SRC.NUM_DAYS_OF_SUPPLY          AS F_FACT_COL1   
                ,SRC.COUNT_OVERSTOCK             AS F_FACT_COL2   
                ,SRC.AVG_L4W_SLS_QTY             AS F_FACT_COL3   
                ,SRC.AVG_L4W_SLS_RTL             AS F_FACT_COL4   	
                ,SRC.AVG_L4W_SLS_CST             AS F_FACT_COL5    
                ,SRC.DAYS_SINCE_SALE             AS F_FACT_COL6   
            FROM DW_DWH.DWH_F_INV_OVERSTK_ILD_B SRC
            INNER JOIN DW_DWH.DWH_D_PRD_ITM_LU ITM ON ITM.ITM_KEY = SRC.ITM_KEY
            INNER JOIN DW_DWH.DWH_D_ORG_LOC_LU LOC ON LOC.LOC_KEY = SRC.LOC_KEY
            --WHERE SRC.DAY_KEY <> '2023-12-13'
            ;
        '''
    delete_query_log_prefix = f"""Attempt for datamart delete query \n {delete_query}"""
    dm_ins_query_log_prefix = f"""Attempt for datamart insert query \n {dm_ins_query}"""
    delete_query_status,del_query_result = execute_query(cursor, delete_query, delete_query_log_prefix,attempt_date,overstk_logger)
    dm_ins_query_status,ins_query_result = execute_query(cursor, dm_ins_query, dm_ins_query_log_prefix,attempt_date,overstk_logger)
    if delete_query_status == dm_ins_query_status == 'Success':
        overstk_logger.info(f'Overstock History datamart load Successful. Deleted result: {del_query_result} and ins result: {ins_query_result}')
    else:
        overstk_logger.error(f'{delete_query_status} and {dm_ins_query_status}')


def overstk_eoh_hist_dm(cursor):
    pass

def overstk_eoh_hist_dm_old(cursor,test_time):
    # Set up logging for overstock
    overstk_eoh_handler = logging.FileHandler(f'{log_file_path}/overstock_eoh_hist.log', encoding='utf-8')
    formatter = CustomFormatter()
    overstk_eoh_handler.setFormatter(formatter)
    overstk_eoh_logger = logging.getLogger('overstock_eoh')
    overstk_eoh_logger.setLevel(logging.DEBUG)
    overstk_eoh_logger.addHandler(overstk_eoh_handler)
     # Set the start and end dates for historical processing
    start_date = '2023-01-01'
    end_date = '2023-01-10'
    eoh_tbl_ddls =  ['''CREATE OR 
                     REPLACE TABLE DW_TMP.TMP_F_INV_OVERSTK_MEAS_FACT_ILD_B_HIST_FIX 
                     CLONE DW_TMP.TMP_F_INV_OVERSTK_MEAS_FACT_ILD_B''',
                     '''
                    CREATE OR REPLACE TABLE DM_MERCH.DM_F_MEAS_FACT_ILD_B_OVERSTK CLONE DM_MERCH.DM_F_MEAS_FACT_ILD_B
                    ''']
    for ddl in eoh_tbl_ddls:
        execute_query(cursor,ddl,f'Attempt for ddl query : {ddl}',start_date,overstk_eoh_logger)
    # Check max load date
    max_dt_sql = '''SELECT MAX(MEAS_DT) FROM DM_MERCH.DM_F_MEAS_FACT_ILD_B_OVERSTK WHERE FACT_CDE = 1650;'''
    print(f'I am here  bro::: ::')
    max_dt_status,date_result = execute_query(cursor,max_dt_sql,f'Checking for Max date from overstock eoh datamart\n {max_dt_sql}',start_date,overstk_eoh_logger)
    # cursor.execute(max_dt_sql)
    date_result = date_result[0][0]
    # date_result = cursor.fetchall()[0][0]
    if date_result is not None:
        max_dt = date_result.strftime('%Y-%m-%d')
    else:
        max_dt = None
    if max_dt == '2023-12-18':
        sys.exit("Max load date is 2023-12-18 so halting the load")
    current_date = start_date
    # Truncate tables
    tables_to_truncate = [
        'DM_MERCH.DM_F_MEAS_FACT_ILD_B_OVERSTK',
        'DW_TMP.TMP_F_INV_OVERSTK_MEAS_FACT_ILD_B_HIST_FIX'
    ]
    truncate_tables(cursor, tables_to_truncate,current_date,overstk_eoh_logger)

    # Process date range
    print(f'Process date range should start')
    pbar = tqdm(total=(datetime.strptime(end_date, '%Y-%m-%d') - datetime.strptime(start_date, '%Y-%m-%d')).days + 1,
                desc='Processing Dates')
    while current_date <= end_date:
        print(f'This is processing for current date: {current_date}')
        date_sql = f'''SELECT DAY_KEY, DAY_KEY-1 AS YTD_DAY_KEY, MTH_START_DT AS FULL_SNAPSHOT_DT
                        FROM DW_DWH.DWH_D_TIM_DAY_LU 
                        WHERE DAY_KEY = '{current_date}';'''

        max_dt_status,date_sql_result = execute_query(cursor,date_sql,f'Checking for yesterday date and full_snapshot_dt\n {date_sql}',current_date,overstk_eoh_logger)
        # cursor.execute(date_sql)
        # date_sql_result = cursor.fetchall()
        day_key = date_sql_result[0][0]
        ytd_day_key = date_sql_result[0][1]
        full_snapshot_dt = date_sql_result[0][2]

        # day_key = None
        # ytd_day_key = None 
        # full_snapshot_dt = None
        overstk_eoh_logger.info(f'''
        day_key : {day_key}
        ytd_day_key : {ytd_day_key}
        full_snapshot_dt : {full_snapshot_dt}
        ''')

        # Truncate temp table
        truncate_query = 'TRUNCATE TABLE DW_TMP.TMP_F_INV_OVERSTK_MEAS_FACT_ILD_B_HIST_FIX;'
        log_prefix = 'Attempt for temp table truncate'
        status = execute_query(cursor, truncate_query, log_prefix,current_date,overstk_eoh_logger)

        # Query 1: Insert into temp table for historical date range
        query_1 = f"""
                INSERT INTO DW_TMP.TMP_F_INV_OVERSTK_MEAS_FACT_ILD_B_HIST_FIX (
                    MEAS_DT, DIV_KEY, CHN_KEY, CHNL_ID, ITM_KEY, LOC_KEY, FACT_CDE, LCL_CNCY_CDE,
                    F_FACT_COL1, F_FACT_COL2, F_FACT_COL3
                )
                SELECT
                    OVERSTK.MEAS_DT AS MEAS_DT,
                    DIV_KEY AS DIV_KEY,
                    CHN_KEY AS CHN_KEY,
                    CHNL_ID AS CHNL_ID,
                    OVERSTK.ITM_KEY AS ITM_KEY,
                    OVERSTK.LOC_KEY AS LOC_KEY,
                    1650 AS FACT_CDE,
                    OVERSTK.LCL_CNCY_CDE AS LCL_CNCY_CDE,
                    -F_FACT_COL1 AS F_FACT_COL1,
                    -F_FACT_COL2 AS F_FACT_COL2,
                    -F_FACT_COL3 AS F_FACT_COL3
                FROM
                    DM_MERCH.DM_F_MEAS_FACT_ILD_B_OVERSTK OVERSTK
                WHERE
                    MEAS_DT BETWEEN '{full_snapshot_dt}' AND '{ytd_day_key}' AND FACT_CDE = 1650;
            """
        log_prefix = f"""Attempt for Query 1 (Insert into temp table) \n {query_1}"""
        status = execute_query(cursor, query_1, log_prefix,current_date,overstk_eoh_logger)

        # Query 2: Insert into temp table for historical date range
        query_2 = f"""
                INSERT INTO DW_TMP.TMP_F_INV_OVERSTK_MEAS_FACT_ILD_B_HIST_FIX (
                    MEAS_DT, DIV_KEY, CHN_KEY, CHNL_ID, ITM_KEY, LOC_KEY, FACT_CDE, LCL_CNCY_CDE,
                    F_FACT_COL1, F_FACT_COL2, F_FACT_COL3
                )
                SELECT
                    DAY_KEY AS MEAS_DT,
                    ITM.DIV_KEY AS DIV_KEY,
                    LOC.CHN_KEY AS CHN_KEY,
                    LOC.CHNL_ID AS CHNL_ID,
                    ITM.ITM_KEY AS ITM_KEY,
                    LOC.LOC_KEY AS LOC_KEY,
                    1650 AS FACT_CDE,
                    LCL_CNCY_CDE AS LCL_CNCY_CDE,
                    OVERSTOCK_INV_QTY AS F_FACT_COL1,
                    OVERSTOCK_INV_CST AS F_FACT_COL2,
                    OVERSTOCK_INV_RTL AS F_FACT_COL3
                FROM
                    DW_DWH.DWH_F_INV_OVERSTK_ILD_B OVERSTK
                INNER JOIN
                    DW_DWH.DWH_D_PRD_ITM_LU ITM ON ITM.ITM_KEY = OVERSTK.ITM_KEY
                INNER JOIN
                    DW_DWH.DWH_D_ORG_LOC_LU LOC ON LOC.LOC_KEY = OVERSTK.LOC_KEY
                WHERE
                    DAY_KEY = '{current_date}';
            """
        log_prefix = f'''Attempt for Query 2 (Insert into temp table) \n {query_2}'''
        status = execute_query(cursor, query_2, log_prefix,current_date,overstk_eoh_logger)

        # DM insert
        query_3 = f"""
              INSERT INTO DM_MERCH.DM_F_MEAS_FACT_ILD_B_OVERSTK (
                  MEAS_DT, DIV_KEY, CHN_KEY, CHNL_ID, ITM_KEY, LOC_KEY, FACT_CDE, LCL_CNCY_CDE,
                  F_FACT_COL1, F_FACT_COL2, F_FACT_COL3
              )
              SELECT
                  '{current_date}',
                  DIV_KEY AS DIV_KEY,
                  CHN_KEY AS CHN_KEY,
                  CHNL_ID AS CHNL_ID,
                  ITM_KEY AS ITM_KEY,
                  LOC_KEY AS LOC_KEY,
                  FACT_CDE AS FACT_CDE,
                  LCL_CNCY_CDE AS LCL_CNCY_CDE,
                  SUM(F_FACT_COL1) AS F_FACT_COL1,
                  SUM(F_FACT_COL2) AS F_FACT_COL2,
                  SUM(F_FACT_COL3) AS F_FACT_COL3
              FROM
                  DW_TMP.TMP_F_INV_OVERSTK_MEAS_FACT_ILD_B_HIST_FIX TMP
              GROUP BY
                  TMP.MEAS_DT, DIV_KEY, CHN_KEY, CHNL_ID, ITM_KEY, LOC_KEY, FACT_CDE, LCL_CNCY_CDE
              HAVING
                  SUM(F_FACT_COL1) <> 0 OR SUM(F_FACT_COL2) <> 0 OR SUM(F_FACT_COL3) <> 0
          """
        log_prefix = f'''Attempt for DM insert (Insert into DM table) {query_3}'''
        status = execute_query(cursor, query_3, log_prefix,current_date,overstk_eoh_logger)

        # Increment the date for the next iteration
        current_date = (datetime.strptime(current_date, '%Y-%m-%d') +
                        timedelta(days=1)).strftime('%Y-%m-%d')
        pbar.update(1)
    pbar.close()
