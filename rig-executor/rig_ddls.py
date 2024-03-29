#################### RETAIL INSIGHT GENERATOR ####################
#################### ELIGIBILITY TABLE ####################
dwh_ddls = [
    '''CREATE OR REPLACE
      TABLE DW_DWH.DWH_D_ELGBL_STY_COL_LOC_MTX(
     LOC_ID VARCHAR
     ,LOC_KEY NUMBER
     ,STY_ID VARCHAR
     ,STY_KEY NUMBER
     ,COLOR_ID VARCHAR
     ,COLOR_KEY NUMBER
     ,INV_SLS_LY_FLG VARCHAR
     ,RCD_INS_TS TIMESTAMP_LTZ(9) NOT NULL
 	,RCD_UPD_TS TIMESTAMP_LTZ(9) NOT NULL
     );''',

    #################### LAST 4 WEEKS SALES ####################
    '''CREATE OR REPLACE
      TABLE DW_DWH.DWH_F_AVG_SLS_L4W_ILD_B (
 	DAY_KEY DATE
 	,ITM_ID VARCHAR(16777216)
 	,ITM_KEY NUMBER(38,0)
 	,LOC_ID VARCHAR(16777216)
 	,LOC_KEY NUMBER(38,0)
 	,AVG_L4W_SLS_QTY NUMBER(18,4)
     ,AVG_L4W_SLS_RTL_LCL NUMBER(18,4)
 	,AVG_L4W_SLS_CST_LCL NUMBER(18,4)
     ,RCD_INS_TS TIMESTAMP_LTZ(9) NOT NULL
 	,RCD_UPD_TS TIMESTAMP_LTZ(9) NOT NULL
 );''',

    #################### IMMINENT STOCKOUT ####################
    '''CREATE OR REPLACE
      TABLE DW_DWH.DWH_F_INV_IMMNT_STKOUT_ILD_B (
 	DAY_KEY DATE
     ,ITM_ID VARCHAR
 	,ITM_KEY NUMBER(38, 0)
     ,LOC_ID VARCHAR
 	,LOC_KEY NUMBER(38, 0)
 	,COUNT_IMMIN_STOCKOUT NUMBER(20, 0)
 	,AVG_L4W_SLS_QTY NUMBER(18, 4)
 	,AVG_L4W_SLS_RTL NUMBER(18, 4)
 	,AVG_L4W_SLS_CST NUMBER(18, 4)
 	,IMMIN_STOCKOUT_THRESHOLD NUMBER(38, 0)
 	,NUM_DAYS_OF_SUPPLY NUMBER(18, 4)
 	,LCL_CNCY_CDE VARCHAR
 	,RCD_INS_TS TIMESTAMP_LTZ(9) NOT NULL
 	,RCD_UPD_TS TIMESTAMP_LTZ(9) NOT NULL
 	);
  ''',

    #################### STOCKOUT ####################
    '''CREATE OR REPLACE
 	 TABLE DW_DWH.DWH_F_INV_STKOUT_ILD_B (
 	DAY_KEY DATE
     ,ITM_ID VARCHAR
 	,ITM_KEY NUMBER(38, 0)
     ,LOC_ID VARCHAR
 	,LOC_KEY NUMBER(38, 0)
 	,COUNT_STOCKOUT NUMBER(38, 0)
 	,COUNT_DAYS NUMBER(38, 0)
 	,COUNT_WEEKEND_DAYS NUMBER(38, 0)
 	,AVG_L4W_SLS_QTY NUMBER(18, 4)
 	,AVG_L4W_SLS_CST NUMBER(18, 4)
 	,AVG_L4W_SLS_RTL NUMBER(18, 4)
 	,LCL_CNCY_CDE VARCHAR(5)
 	,RCD_INS_TS TIMESTAMP_LTZ(9) NOT NULL
 	,RCD_UPD_TS TIMESTAMP_LTZ(9) NOT NULL
 	);
  ''',

    #################### OVERSTOCKS ####################
    '''CREATE OR REPLACE
       TABLE DW_DWH.DWH_F_INV_OVERSTK_ILD_B (
      DAY_KEY DATE
      ,LOC_ID VARCHAR
      ,LOC_KEY NUMBER(38, 0)
      ,ITM_ID VARCHAR
      ,ITM_KEY NUMBER(38, 0)
      ,COUNT_OVERSTOCK NUMBER(38, 0)
      ,OVERSTOCK_DAY_THRESHOLD NUMBER(38, 1)
      ,AVG_L4W_SLS_QTY NUMBER(18, 4)
      ,AVG_L4W_SLS_CST NUMBER(18, 4)
      ,AVG_L4W_SLS_RTL NUMBER(18, 4)
      ,NUM_DAYS_OF_SUPPLY NUMBER(18, 4)
      ,DAYS_SINCE_SALE NUMBER(38, 0)
      ,OVERSTOCK_INV_RTL NUMBER(18, 4)
      ,OVERSTOCK_INV_QTY NUMBER(38, 0)
      ,OVERSTOCK_INV_CST NUMBER(18, 4)
      ,LCL_CNCY_CDE VARCHAR
      ,RCD_INS_TS TIMESTAMP_LTZ(9) NOT NULL
 	,RCD_UPD_TS TIMESTAMP_LTZ(9) NOT NULL
  );
  '''
]

####################### DWH VIEW DDL ######################

dwh_v_ddls = [
    '''CREATE OR REPLACE
 VIEW DW_DWH_V.V_DWH_D_ELGBL_STY_COL_LOC_MTX(
	LOC_ID,
	LOC_KEY,
	STY_ID,
	STY_KEY,
	COLOR_ID,
	COLOR_KEY,
	INV_SLS_LY_FLG,
	RCD_INS_TS,
	RCD_UPD_TS
) AS
SELECT
	LOC_ID,
	LOC_KEY,
	STY_ID,
	STY_KEY,
	COLOR_ID,
	COLOR_KEY,
	INV_SLS_LY_FLG,
	RCD_INS_TS,
	RCD_UPD_TS
FROM DW_DWH.DWH_D_ELGBL_STY_COL_LOC_MTX;
''', '''CREATE OR REPLACE
 VIEW DW_DWH_V.V_DWH_F_AVG_SLS_L4W_ILD_B(
	DAY_KEY,
	ITM_ID,
	ITM_KEY,
	LOC_ID,
	LOC_KEY,
	AVG_L4W_SLS_QTY,
	AVG_L4W_SLS_RTL_LCL,
	AVG_L4W_SLS_CST_LCL,
	RCD_INS_TS,
	RCD_UPD_TS
) AS
SELECT
	DAY_KEY,
	ITM_ID,
	ITM_KEY,
	LOC_ID,
	LOC_KEY,
	AVG_L4W_SLS_QTY,
	AVG_L4W_SLS_RTL_LCL,
	AVG_L4W_SLS_CST_LCL,
	RCD_INS_TS,
	RCD_UPD_TS
FROM DW_DWH.DWH_F_AVG_SLS_L4W_ILD_B;
''', '''CREATE OR REPLACE  VIEW DW_DWH_V.V_DWH_F_INV_IMMNT_STKOUT_ILD_B(
	DAY_KEY,
	ITM_ID,
	ITM_KEY,
	LOC_ID,
	LOC_KEY,
	COUNT_IMMIN_STOCKOUT,
	AVG_L4W_SLS_QTY,
	AVG_L4W_SLS_RTL,
	AVG_L4W_SLS_CST,
	IMMIN_STOCKOUT_THRESHOLD,
	NUM_DAYS_OF_SUPPLY,
	LCL_CNCY_CDE,
	RCD_INS_TS,
	RCD_UPD_TS
) AS
SELECT
 	DAY_KEY,
	ITM_ID,
	ITM_KEY,
	LOC_ID,
	LOC_KEY,
	COUNT_IMMIN_STOCKOUT,
	AVG_L4W_SLS_QTY,
	AVG_L4W_SLS_RTL,
	AVG_L4W_SLS_CST,
	IMMIN_STOCKOUT_THRESHOLD,
	NUM_DAYS_OF_SUPPLY,
	LCL_CNCY_CDE,
	RCD_INS_TS,
	RCD_UPD_TS
FROM DW_DWH.DWH_F_INV_IMMNT_STKOUT_ILD_B;
''', '''CREATE OR REPLACE  VIEW DW_DWH_V.V_DWH_F_INV_STKOUT_ILD_B(
	DAY_KEY,
	ITM_ID,
	ITM_KEY,
	LOC_ID,
	LOC_KEY,
	COUNT_STOCKOUT,
	COUNT_DAYS,
	COUNT_WEEKEND_DAYS,
	AVG_L4W_SLS_QTY,
	AVG_L4W_SLS_CST,
	AVG_L4W_SLS_RTL,
	LCL_CNCY_CDE,
	RCD_INS_TS,
	RCD_UPD_TS
) AS
SELECT DAY_KEY,
	ITM_ID,
	ITM_KEY,
	LOC_ID,
	LOC_KEY,
	COUNT_STOCKOUT,
	COUNT_DAYS,
	COUNT_WEEKEND_DAYS,
	AVG_L4W_SLS_QTY,
	AVG_L4W_SLS_CST,
	AVG_L4W_SLS_RTL,
	LCL_CNCY_CDE,
	RCD_INS_TS,
	RCD_UPD_TS
FROM DW_DWH.DWH_F_INV_STKOUT_ILD_B;
''', '''CREATE OR REPLACE
 VIEW DW_DWH_V.V_DWH_F_INV_OVERSTK_ILD_B(
	DAY_KEY,
	LOC_ID,
	LOC_KEY,
	ITM_ID,
	ITM_KEY,
	COUNT_OVERSTOCK,
	OVERSTOCK_DAY_THRESHOLD,
	AVG_L4W_SLS_QTY,
	AVG_L4W_SLS_CST,
	AVG_L4W_SLS_RTL,
	NUM_DAYS_OF_SUPPLY,
	DAYS_SINCE_SALE,
	OVERSTOCK_INV_RTL,
	OVERSTOCK_INV_QTY,
	OVERSTOCK_INV_CST,
	LCL_CNCY_CDE,
	RCD_INS_TS,
	RCD_UPD_TS
) AS
SELECT
 DAY_KEY,
	LOC_ID,
	LOC_KEY,
	ITM_ID,
	ITM_KEY,
	COUNT_OVERSTOCK,
	OVERSTOCK_DAY_THRESHOLD,
	AVG_L4W_SLS_QTY,
	AVG_L4W_SLS_CST,
	AVG_L4W_SLS_RTL,
	NUM_DAYS_OF_SUPPLY,
	DAYS_SINCE_SALE,
	OVERSTOCK_INV_RTL,
	OVERSTOCK_INV_QTY,
	OVERSTOCK_INV_CST,
	LCL_CNCY_CDE,
	RCD_INS_TS,
	RCD_UPD_TS
FROM DW_DWH.DWH_F_INV_OVERSTK_ILD_B;
'''
]
#################### Datamart view of eligibility using last 4 weeks average sales table ##############

dm_view_ddls = [
    '''
CREATE OR REPLACE  view DM_MERCH_V.DV_DWH_D_ELGBL_STY_COL_ITM_LOC_MTX(
	ITM_ID,
	ITM_KEY,
	LOC_ID,
	LOC_KEY,
	STY_KEY,
	STY_ID,
	COLOR_KEY,
	COLOR_ID
) AS
SELECT 
SLS.ITM_ID
,SLS.ITM_KEY
,LOC_ID
,LOC_KEY
,ITM.STY_KEY
,ITM.STY_ID
,ITM.COLOR_KEY
,ITM.COLOR_ID
FROM DW_DWH.DWH_F_AVG_SLS_L4W_ILD_B SLS 
JOIN DW_DWH.DWH_D_PRD_ITM_LU ITM 
ON SLS.ITM_KEY = ITM.ITM_KEY
INNER JOIN DW_DWH.DWH_D_CURR_TIM_LU CURR ON SLS.DAY_KEY >= to_date(to_char(dateadd(week, -52, curr_day),'YYYY-MM-01'))
GROUP BY ALL;
'''
]
############ TEMP DDL OF OVERSTOCK EOH ################

tmp_ddls = [
    '''CREATE OR REPLACE 
 TABLE DW_TMP.TMP_F_INV_OVERSTK_EOH_MEAS_FACT_ILD_B (
	MEAS_DT DATE,
	CHN_KEY NUMBER,
	CHNL_ID VARCHAR,
	COMPANY_KEY NUMBER,
	DIV_KEY NUMBER,
	ITM_KEY NUMBER,
	LOC_KEY NUMBER,
	FROM_LOC_KEY NUMBER,
	RSN_KEY NUMBER,
	MIN_KEY NUMBER,
	SUP_KEY NUMBER,
	ITMLOC_STTS_CDE VARCHAR,
	RTL_TYP_CDE VARCHAR,
	LOC_TYP_CDE VARCHAR,
	DSC_TYP_CDE VARCHAR,
	RTRN_FLG NUMBER,
	CUS_KEY NUMBER,
	TXN_ID VARCHAR,
	PRM_KEY NUMBER,
	PO_NUM NUMBER,
	CO_HDR_KEY NUMBER,
	EXP_RCT_DT DATE,
	ORD_PLACED_DT DATE,
	ORD_BACKORDERED_DT DATE,
	FACT_CDE NUMBER,
	LCL_CNCY_CDE VARCHAR,
	F_FACT_QTY NUMBER(18,4),
	F_FACT_CST NUMBER(18,4),
	F_FACT_RTL NUMBER(18,4),
	F_FACT_COL1 NUMBER(18,4),
	F_FACT_COL2 NUMBER(18,4),
	F_FACT_COL3 NUMBER(18,4),
	F_FACT_COL4 NUMBER(18,4),
	F_FACT_COL5 NUMBER(18,4),
	F_FACT_COL6 NUMBER(18,4),
	F_FACT_COL7 NUMBER(18,4),
	F_FACT_COL8 NUMBER(18,4),
	ORIG_LOC_KEY NUMBER,
	HT_CLR_MKDN_TYPE VARCHAR,
	HT_PO_TYP_CDE VARCHAR,
	HT_DMND_SRC_KEY NUMBER,
	HT_FULFILL_TYP_KEY NUMBER,
	HT_TXN_TYP_CDE VARCHAR,
	SELLING_UOM VARCHAR,
	MULTI_SELLING_UOM VARCHAR,
	HT_ORG_CNTRY_CDE VARCHAR,
	DMND_LOC_KEY NUMBER,
	TNDR_KEY NUMBER,
	SHIPPED_DT DATE,
	ZIP_CDE VARCHAR,
	CUS_LOYALTY_ID VARCHAR,
	EMP_KEY NUMBER,
	CUS_ORD_ID VARCHAR,
	POS_TXN_ID VARCHAR,
	PR_CDE VARCHAR
);
'''
]

fact_tim_rul_mtx_ins = [
    '''
INSERT INTO DM_MERCH.DM_D_MEAS_FACT_TIM_RULE_MTX (
	FACT_CDE
	,MEAS_TIM_RULE_CDE
	,MEAS_CDE
	,MEAS_COEFF
	)
SELECT * FROM (
SELECT 
	1700 AS FACT_CDE
	,10 AS MEAS_TIM_RULE_CDE
	,'INV_IMMNT_STKOUT' AS MEAS_CDE
	,1 AS MEAS_COEFF
    UNION ALL 
SELECT 1650
	,20
	,'INV_OVERSTK_EOH'
	,1
UNION ALL 
SELECT 
	1600
	,10
	,'INV_OVERSTK'
	,1
UNION ALL 
SELECT 
	1500
	,10
	,'INV_STKOUT'
	,1
UNION ALL
SELECT 
	110
	,140
	,'SLS_AVGL4W'
	,1
    ) SRC
WHERE NOT EXISTS (SELECT *
                  FROM  DM_MERCH.DM_D_MEAS_FACT_TIM_RULE_MTX  AS tgt
                  WHERE tgt.FACT_CDE = SRC.FACT_CDE);

    '''
]

c_batch_scripts_ins = [
    '''

INSERT INTO DW_DWH.DWH_C_BATCH_SCRIPTS 
    SELECT * FROM (
    SELECT 'd_elgbl_sty_col_loc_mtx_ld'  AS SCRIPT_NAME
	,'Script to load eligible style color location matrix' AS SCRIPT_DESC
	,'NTLY' AS MODULE_TYP
	,'Nightly' AS MODULE_DESC
	,'NA' AS MODULE_LOAD_TYP
	,max(nvl(job_id, 0)) + 1 AS JOB_ID FROM DW_DWH.DWH_C_BATCH_SCRIPTS

----Seed script for loading Last 4 weeks average Sales----
    union all
    SELECT 'f_avg_sls_l4w_ild_ld'
	,'Script to load Last 4 weeks average Sales'
	,'NTLY'
	,'Nightly'
	,'NA'
	,max(nvl(job_id, 0)) + 1 FROM DW_DWH.DWH_C_BATCH_SCRIPTS
----Seed script for loading imminent stockout----
    union all
    SELECT 'f_inv_immnt_stkout_ild_ld'
	,'Script to load imminent stockout by item/loc/day'
	,'NTLY'
	,'Nightly'
	,'NA'
	,max(nvl(job_id, 0)) + 1 FROM DW_DWH.DWH_C_BATCH_SCRIPTS
	
----Seed script for loading imminent stockout datamart----
    union all 
    SELECT 'dm_f_inv_immnt_stkout_meas_fact_ild'
	,'Script to load imminent stockout datamart by item/loc/day'
	,'NTLY'
	,'Nightly'
	,'NA'
	,max(nvl(job_id, 0)) + 1 FROM DW_DWH.DWH_C_BATCH_SCRIPTS

----Seed script for loading stockout----
    union all 

    SELECT 'f_inv_stkout_ild_ld'
	,'Script to load stockouts by itm/loc/day'
	,'NTLY'
	,'Nightly'
	,'NA'
	,max(nvl(job_id, 0)) + 1 FROM DW_DWH.DWH_C_BATCH_SCRIPTS


----Seed script for loading stockout datamart----
	    union all 

    SELECT 'dm_f_inv_stkout_meas_fact_ild'
	,'Script to load stockout datamart by item/loc/day'
	,'NTLY'
	,'Nightly'
	,'NA'
	,max(nvl(job_id, 0)) + 1 FROM DW_DWH.DWH_C_BATCH_SCRIPTS


----Seed script for loading overstock----
	    union all 

    SELECT 'f_inv_overstk_ild_ld'
	,'Script to load data for overstocks by item/loc/day'
	,'NTLY'
	,'Nightly'
	,'NA'
	,max(nvl(job_id, 0)) + 1 FROM DW_DWH.DWH_C_BATCH_SCRIPTS

----Seed script for loading overstock datamart from dwh table----
	union all
    SELECT 'dm_f_inv_overstk_meas_fact_ild'
	,'Script to load datamart for overstocks by item/loc/day'
	,'NTLY'
	,'Nightly'
	,'NA'
	,max(nvl(job_id, 0)) + 1 FROM DW_DWH.DWH_C_BATCH_SCRIPTS

----Seed script for loading overstock eoh facts datamart from dwh and tmp table----
	    union all 

    SELECT 'dm_f_inv_overstk_eoh_meas_fact_ild'
	,'Script to load datamart for overstocks eoh facts by item/loc/day'
	,'NTLY'
	,'Nightly'
	,'NA'
	,max(nvl(job_id, 0)) + 1 FROM DW_DWH.DWH_C_BATCH_SCRIPTS
	)
    SRC 
    WHERE NOT EXISTS (
        SELECT * FROM DW_DWH.DWH_C_BATCH_SCRIPTS TGT 
        WHERE SRC.SCRIPT_NAME = TGT.SCRIPT_NAME
    );
'''
]

hist_to_curr_ins_queries = [
    '''
	---insert avg l4w sls history to daily dwh table from hist table
	INSERT INTO DW_DWH.DWH_F_AVG_SLS_L4W_ILD_B
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
SELECT 
DAY_KEY,
              ITM_ID,
              ITM_KEY,
              LOC_ID,
              LOC_KEY,
              AVG_L4W_SLS_QTY,
              AVG_L4W_SLS_RTL_LCL,
              AVG_L4W_SLS_CST_LCL,
              RCD_INS_TS,
          RCD_UPD_TS
FROM DW_DWH.DWH_F_AVG_SLS_L4W_ILD_B_HIST
	''', '''
	---insert immnt stkout history to daily dwh table from hist table
	INSERT INTO DW_DWH.DWH_F_INV_IMMNT_STKOUT_ILD_B
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
	SELECT 
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
	FROM DW_DWH.DWH_F_INV_IMMNT_STKOUT_ILD_B_HIST;
	''', '''
	---insert STKOUT history to daily dwh table from hist table
	INSERT INTO DW_DWH.DWH_F_INV_STKOUT_ILD_B (
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
		SELECT 
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
		FROM DW_DWH.DWH_F_INV_STKOUT_ILD_B_HIST;
	''', '''
	---insert OVERSTK history to daily dwh table from hist table
	INSERT INTO DW_DWH.DWH_F_INV_OVERSTK_ILD_B(
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
		SELECT 
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
		FROM DW_DWH.DWH_F_INV_OVERSTK_ILD_B_HIST;
	'''
]

#################### QUINTILES TMP DDL ####################
qntiles_tmp_ddl = [
    ################# PRODUCT QUINTILE ###################
    '''
CREATE
OR REPLACE TABLE DW_TMP.TMP_F_STY_COLOR_QUINTILES_B (
WK_KEY NUMBER(38, 0)
,DPT_ID VARCHAR
,DPT_KEY NUMBER(38, 0)
,STY_ID VARCHAR
,STY_KEY NUMBER(38, 0)
,COLOR_ID VARCHAR
,COLOR_KEY NUMBER(38, 0)
,RANK_BY_SLS NUMBER(38, 0)
,CNT_BY_DPT NUMBER(38, 0)
);
''',
    ##################### TXN QUINTILE ###################
    '''
CREATE OR REPLACE TABLE DW_TMP.TMP_F_SLS_TXN_QUINTILES_B (
  DAY_KEY DATE,
  TXN_ID VARCHAR(16777216),
  RANK_BY_SLS NUMBER(38,0),
  CNT_TXN_BY_DAY NUMBER(38,0)
);
'''
]
#################### QUINTILES DWH DDL ####################
qntiles_dwh_ddl = [
    ##################### TXN QUINTILE ###################
    '''
  CREATE OR REPLACE TABLE DW_DWH.DWH_F_SLS_TXN_QUINTILES_B (
    DAY_KEY DATE,
    TXN_ID VARCHAR(16777216),
    QTILE_SLS NUMBER(10,6),
    CNT_BY_TXN NUMBER(38,0)
  );
''',
    ################# PRODUCT QUINTILE ###################
    '''
CREATE
OR REPLACE TABLE DW_DWH.DWH_F_STY_COLOR_QUINTILES_B (
WK_KEY NUMBER(38, 0)
,DPT_ID VARCHAR
,DPT_KEY NUMBER(38, 0)
,STY_ID VARCHAR
,STY_KEY NUMBER(38, 0)
,COLOR_ID VARCHAR
,COLOR_KEY NUMBER(38, 0)
,QTILE_SLS_L4W NUMBER(10, 6)
,CNT_BY_STY_COLOR NUMBER(38, 0)
);
'''
]

#################### QUINTILES DWH VIEW DDL ####################
qntiles_view_ddl = [
    ##################### TXN QUINTILE ###################
    '''
CREATE OR REPLACE VIEW DW_DWH_V.V_DWH_F_SLS_TXN_QUINTILES_B
(
  DAY_KEY,
  TXN_ID,
  QTILE_SLS,
  CNT_BY_TXN
) AS 
SELECT 
DAY_KEY,
TXN_ID,
QTILE_SLS,
CNT_BY_TXN
FROM DW_DWH.DWH_F_SLS_TXN_QUINTILES_B;
''',
    ################# PRODUCT QUINTILE ###################
    '''
CREATE
  OR REPLACE VIEW DW_DWH_V.V_DWH_F_STY_COLOR_QUINTILES_B AS

SELECT *
FROM DW_DWH.DWH_F_STY_COLOR_QUINTILES_B;
'''
]

qntiles_dm_view_ddl = [
    ################# PRODUCT QUINTILE ###################
    '''
  CREATE
    OR REPLACE VIEW DM_MERCH_V.DV_DWH_F_STY_COLOR_QUINTILES_B AS

  SELECT *
    ,CASE
      WHEN QTILE_SLS_L4W <= 0.2
        AND QTILE_SLS_L4W <> 0
        THEN 'Quintile 1'
      WHEN QTILE_SLS_L4W <= 0.4
        AND QTILE_SLS_L4W > 0.2
        THEN 'Quintile 2'
      WHEN QTILE_SLS_L4W <= 0.6
        AND QTILE_SLS_L4W > 0.4
        THEN 'Quintile 3'
      WHEN QTILE_SLS_L4W <= 0.8
        AND QTILE_SLS_L4W > 0.6
        THEN 'Quintile 4'
      WHEN QTILE_SLS_L4W <= 1
        AND QTILE_SLS_L4W > 0.8
        THEN 'Quintile 5'
      WHEN QTILE_SLS_L4W = 0
        THEN 'Other'
      END AS QUINTILE
  FROM DW_DWH.DWH_F_STY_COLOR_QUINTILES_B;
  ''',
    ##################### TXN QUINTILE ###################
    '''
  CREATE OR REPLACE VIEW DM_MERCH_V.DV_DWH_F_SLS_TXN_QUINTILES_B
  (
    DAY_KEY,
    TXN_ID,
    QTILE_SLS,
    CNT_BY_TXN
  ) AS 
  SELECT 
  DAY_KEY,
  TXN_ID,
  QTILE_SLS,
  CNT_BY_TXN
  FROM DW_DWH.DWH_F_SLS_TXN_QUINTILES_B;
  '''
]
