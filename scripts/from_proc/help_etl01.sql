SELECT DECODE(SUM(DECODE(T.MEMO, 'SUCCESS', 0, 1)), 0, '1', '0') AS TASK_FLAG
  FROM KETTLE.DIM_ETL_SYS_LOG T
 WHERE T.BUSIDATE = '{$tx_date}'
   AND T.SYSNAME IN
       ('TRA2', 'TRA5', 'TRAM', 'TRMG', 'TRMGHIS', 'MDA5', 'MDATA')
       
       
-- dcp
http://172.22.131.50:9077/dcp/login.jsp#
user: admin
pwd : dtct@62936067

-- ariflow
http://172.22.131.20:8080/admin/
pwd:user  null

-- yarn
http://172.22.131.24:9999/#/main/services/YARN/summary
admin/admin


-- create table demo
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonsrict;

spark-sql --master yarn --num-executors 8 --executor-memory 6g --executor-cores 4 --conf spark.sql.shuffle.partitions=10

create table base_data.t_test_dynamic(
id string,
name string
)
PARTITIONED BY (dt_date string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
STORED AS PARQUET
;

insert into table base_data.t_test_dynamic partition(dt_date)
select branch_code, src_branch_code, biz_date
from base_data.tas_fund


--------------------
oracle.jdbc.driver.OracleDriver
jdbc:oracle:thin:@172.22.164.81:1521:siddtct
