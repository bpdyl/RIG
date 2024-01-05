from rig_ddls import *
from database_operations import establish_connection
from utils import execute_query,truncate_tables,CustomFormatter,log_file_path
from elgbl_and_l4w_sls import avg_lw_sls_hist_dwh,elgbl_mtx_load
from immnt_stkout_hist import immnt_stkout_hist_dwh,immnt_stkout_hist_dm
from stkout_hist import stkout_hist_dwh,stkout_hist_dm
from overstk_hist import overstk_hist_dwh,overstk_hist_dm,overstk_eoh_hist_dm
from datetime import datetime,timedelta
import concurrent.futures
import pytz
import logging



tz = pytz.timezone('Asia/Kathmandu')
start_time = datetime.now(tz) 

conn = establish_connection()
cursor = conn.cursor()
# Set up logging for general
rig_executor_handler = logging.FileHandler(f'{log_file_path}/rig_executor.log', encoding='utf-8')
rig_executor_logger = logging.getLogger('rig_executor')
rig_executor_logger.setLevel(logging.DEBUG)
rig_executor_logger.addHandler(rig_executor_handler)
rig_executor_logger.info(f'Run started on {start_time}')

def create_tables_and_views(all_ddls,logger):
    # Set up logging for ddls and insert
    attempt_date = datetime.now(tz)
    for query in all_ddls:
        log_prefix = f'Attempt for DDL queries\n {query}'
        # print(type(query))
        execute_query(cursor,query,log_prefix,attempt_date,logger)

def init_ins_query(init_queries,logger):
    attempt_date = datetime.now(tz)
    for query in init_queries:
        log_prefix = f'Attempt for fact time rule mtx and c-batch scripts insert queries {query}'
        # print(type(query))
        status,res = execute_query(cursor,query,log_prefix,attempt_date,logger)
        print(f'Status: {status} and result : {res}')
        logger.info(f'This is status and result: {status} and {res}')

def hist_to_curr_tbl_insert(hist_to_curr_queries,logger):
    attempt_date = datetime.now(tz)
    for query in hist_to_curr_queries:
        log_prefix = f'Attempt for history to current table insert queries {query}'
        # print(type(query))
        execute_query(cursor,query,log_prefix,attempt_date,logger)

def ddls_and_insert_operation():
    ddls_and_ins_handler = logging.FileHandler(f'{log_file_path}/ddls_and_insert.log', encoding='utf-8')
    formatter = CustomFormatter()
    ddls_and_ins_handler.setFormatter(formatter)
    ddls_and_ins_logger = logging.getLogger('ddls_insert')
    ddls_and_ins_logger.setLevel(logging.DEBUG)
    ddls_and_ins_logger.addHandler(ddls_and_ins_handler)
    #Combine the ddl of dwh, dwh views and temp tables
    rig_ddls = dwh_ddls+dwh_v_ddls+dm_view_ddls+tmp_ddls
    create_tables_and_views(rig_ddls,ddls_and_ins_logger)
    #Combine the insert query of fact time rule mtx seed and c-batch scripts seed
    init_ins_queries = fact_tim_rul_mtx_ins+c_batch_scripts_ins
    init_ins_query(init_ins_queries,ddls_and_ins_logger)


def history_load():
    methods_group_1 = [
        (elgbl_mtx_load,(cursor,start_time))
    ]

    methods_group_2 = [
        (avg_lw_sls_hist_dwh,(cursor,start_time))
    ]

    methods_group_3 = [
        (immnt_stkout_hist_dwh,(cursor,start_time)),
        (stkout_hist_dwh,(cursor,start_time)),
        (overstk_hist_dwh,(cursor,start_time))
    ]

    methods_group_4 = [
        (overstk_eoh_hist_dm,(cursor,start_time)),
        (immnt_stkout_hist_dm,(cursor,start_time)),
        (stkout_hist_dm,(cursor,start_time)),
        (overstk_hist_dm,(cursor,start_time))
    ]

    # Create a ThreadPoolExecutor
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Submit the methods for execution
        futures_group_1 = {executor.submit(method, *args) for method, args in methods_group_1}
        concurrent.futures.wait(futures_group_1)
        futures_group_2 = {executor.submit(method, *args) for method, args in methods_group_2}
        concurrent.futures.wait(futures_group_2)
        futures_group_3 = {executor.submit(method, *args) for method, args in methods_group_3}
        concurrent.futures.wait(futures_group_3)
        futures_group_4 = {executor.submit(method, *args) for method, args in methods_group_4}
        concurrent.futures.wait(futures_group_4)


## Entry point of the program
def executor():
    ddls_and_insert_operation()
    history_load()
    # overstk_eoh_hist_dm(cursor,start_time)


executor()
end_date = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
status = 'Completed'
rig_executor_logger.info(f'Run completed on {end_date}: {status}')
end_time = datetime.now(tz)
time_difference = end_time - start_time
# Convert the time difference to hours and minutes
hours = time_difference // timedelta(hours=1)
minutes = (time_difference // timedelta(minutes=1)) % 60

rig_executor_logger.info(f'Total time taken {hours} hr: {minutes} min')
# Close the connection
conn.close()
print("Done!")
