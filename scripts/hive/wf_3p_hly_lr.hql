--/***************************************************************************************************
--** Name:		3P Linear Regression Model
--** Version:		1.0
--** Created by:	Melody Zhao
--** Created Date:	09/2014
--** Function:		3p linear regression model based on residential customers' night hourly usage during summer time.
--**
--** Modification:
--** Person		Date		Details
--** Ken Ouyang      2015-03-20    Added partition repair code
--** Ken Ouyang      2015-04-09    Modified and merged to main workflow
--**
--*****************************************************************************************************/

--Set the Hive execution engine to tez
set hive.execution.engine=mr;

--Choose Database
use hvdbrtl${hiveconf:MODE};

--Add jar with NRG LinearRegressor model
add jar /app/${hiveconf:MODE}/summer_regression/lib/hiveUDFs/nrgUDF_3pLR/target/nrgUDF_3pLR.jar;
create temporary function 3pmodel as 'com.nrg.nrgUDF_3pLR.nrgUDF_3pLR';

--Check and add new partitions if exist
msck repair table `hv_l_3p_hly`;

!echo INFO: Creating target table;
create external table if not exists `hv_m_3p_hly_lr`
(ESIID varchar(30), slope string, intcpt string, C2 STRING, BASELINE STRING, AVG_USG string, MAX_USG STRING, MIN_USG STRING, R2_DWDS string, RMSE string, PI_99 string, PI_95 string, PI_90 string, mean_temp string, SS_x string, VALID_COUNT string, BASE_COUNT STRING, REPLICATION_NUM string, PROGRAM_NM string)
partitioned by (effective_dt date)
row format delimited fields terminated by ',' lines terminated by '\n'
location '/bgdrtl${hiveconf:MODE}/models/summer_regression/hv_m_3p_hly_lr/';

!echo INFO: Counting distinct esiids in source table;
--select count(distinct allstring) from hv_l_3p_hly
--where effective_dt = '${hiveconf:RUN_YEAR}-${hiveconf:RUN_MONTH}-${hiveconf:RUN_DATE}';

!echo INFO: Running LinearRegressor on each esiid;
insert overwrite table `hv_m_3p_hly_lr`
partition (EFFECTIVE_DT = '${hiveconf:RUN_YEAR}-${hiveconf:RUN_MONTH}-${hiveconf:RUN_DATE}')
select
  split(B.line, '\\|')[0] as ESIID, 
  split(B.line, '\\|')[1] as SLOPE,
  split(B.line, '\\|')[2] as INTCPT,
  split(B.line, '\\|')[3] as C2,
  split(B.line, '\\|')[4] as BASELINE,
  split(B.line, '\\|')[5] as AVG_USG,
  split(B.line, '\\|')[6] as MAX_USG,
  split(B.line, '\\|')[7] as MIN_USG,
  split(B.line, '\\|')[8] as R2_DWDS,
  split(B.line, '\\|')[9] as RMSE,
  split(B.line, '\\|')[10] as PI_99,
  split(B.line, '\\|')[11] as PI_95,
  split(B.line, '\\|')[12] as PI_90,
  split(B.line, '\\|')[13] as mean_temp,
  split(B.line, '\\|')[14] as SS_x,
  split(B.line, '\\|')[15] as VALID_COUNT,
  split(B.line, '\\|')[16] as BASE_COUNT,
  ${hiveconf:REPLICATION} as REPLICATION_NUM,
  "MAHOUT" as PROGRAM_NM
from (
  select 3pmodel(
    concat_ws(",",
      A.allstring,
      '${hiveconf:CON_LVL}',
      '${hiveconf:REPLICATION}'
    )
  ) as line
  from `hv_l_3p_hly` A
  where A.eff_dt = '${hiveconf:RUN_YEAR}-${hiveconf:RUN_MONTH}-${hiveconf:RUN_DATE}'
) B
;

!echo INFO: Counting distinct esiids in target table;
select count(distinct esiid) from `hv_m_3p_hly_lr`
where effective_dt = '${hiveconf:RUN_YEAR}-${hiveconf:RUN_MONTH}-${hiveconf:RUN_DATE}';