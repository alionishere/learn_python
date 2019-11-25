drop table base_data.tmg_cnt_monitor;
create table base_data.tmg_cnt_monitor (
tb_name string comment'校核表',
tb_type string comment '表类型',
cnt string comment '入库数据行数'
)
PARTITIONED BY (run_date string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
STORED AS PARQUET
;

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonsrict;

drop table base_data.tmg_cnt_ratio_monitor;
create table base_data.tmg_cnt_ratio_monitor (
seq int comment '序列',
tb_name string comment'校核表',
tb_type string comment '表类型',
today_cnt string comment '今日入库数据行数',
yest_cnt string comment '昨日入库数据行数',
ratio double comment '今日和昨日数据变化环比'
)
PARTITIONED BY (run_date string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
STORED AS PARQUET
;



-------------------------------------------
-- oracle
CREATE TABLE BASE_DATA.TMG_TASK_CFG (
TASK_GROUP_NO  VARCHAR2(2),
TASK_ID        VARCHAR2(50),
TASK_TOPIC     VARCHAR2(50),
PRE_TASK_ID    VARCHAR2(50),
TASK_CMD       VARCHAR2(1000)
);
COMMENT ON TABLE TMG_TASK_CFG IS 'airflow任务配置表';
COMMENT ON COLUMN  TMG_TASK_CFG.TASK_GROUP_NO IS '任务组';
COMMENT ON COLUMN  TMG_TASK_CFG.TASK_ID IS '任务编号';
COMMENT ON COLUMN  TMG_TASK_CFG.TASK_TOPIC IS '任务主题';
COMMENT ON COLUMN  TMG_TASK_CFG.PRE_TASK_ID IS '依赖任务编号';
COMMENT ON COLUMN  TMG_TASK_CFG.TASK_CMD IS '任务执行命令';


-------------------------------------------
-- 主键
drop table base_data.tmg_unique_monitor;
create table base_data.tmg_unique_monitor (
tb_name string comment'校核表',
pk string comment '主键',
rp_cnt string comment '主键重复数',
part_date string comment '分区字段'
)
PARTITIONED BY (run_date string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
STORED AS PARQUET
;

drop table base_data.tmg_ck_monitor;
create table base_data.tmg_ck_monitor (
tb_name string comment'校核表',
ck_cols string comment '校核字段',
exp_cnt string comment '数据异常数量',
part_date string comment '分区字段'
)
PARTITIONED BY (run_date string, ck_item string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' 
STORED AS PARQUET
;



















