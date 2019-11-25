mysql://root:Dwzq!951753@127.0.0.1:3306/airflow
mysql://airflow:airflow@127.0.0.1:3306/airflow

mysql -uroot -h172.22.131.20 -p
mysql -uroot -h127.0.0.1 -p
Dwzq!951753

/*
select   ROWNUM,
  SYSNAME,
  BUSIDATE,
  BEGINTIME,
  ENDTIME,
  MEMO from (
select * from KETTLE.DIM_ETL_SYS_LOG where BUSIDATE=${BUSIDATE}　order by begintime)
*/

select (@i:=@i+1) as id
       ,dag_id
       ,task_id
       ,date_format(date_sub(start_date, interval 8 hour), '%Y/%m/%d %H:%i:%S') as start_date
       ,date_format(date_sub(end_date, interval 8 hour), '%Y/%m/%d %H:%i:%S') as end_date
       ,duration
       ,state
       -- ,execution_date
  from airflow.task_instance,(select @i:=0) as t
 where dag_id = 'task_base_data_daily_new'
   and date_format(execution_date, '%Y%m%d') = date_format(date_sub(now(),interval 1 day), '%Y%m%d')
 order by task_id

select dag_id
       ,date_format(date_sub(start_date, interval 8 hour), '%Y/%m/%d %H:%i:%S') as start_date
       ,date_format(date_sub(end_date, interval 8 hour), '%Y/%m/%d %H:%i:%S') as end_date
       ,state
       ,end_date - start_date as duration
  from airflow.dag_run
 where date_format(execution_date, '%Y%m%d') = date_format(date_sub(now(),interval 1 day), '%Y%m%d')


-- airflow 模板
select (@i:=@i+1) as id
       ,'base_data' as dag_id
       ,task_id
       ,date_format(date_sub(start_date, interval 8 hour), '%Y/%m/%d %H:%i:%S') as start_date
       ,date_format(date_sub(end_date, interval 8 hour), '%Y/%m/%d %H:%i:%S') as end_date
       ,duration
       ,state
  from airflow.task_instance,(select @i:=0) as t
 where dag_id = 'task_base_data_daily_new'
   and date_format(execution_date, '%Y%m%d') = ${execution_date}
   and task_id like '%${task_id}%'
 order by task_id

 -- 自定义控件参数
 SQL("kettle","select max(rq) from kettle.jyr where rq<to_char(sysdate,'yyyymmdd') and jyrbs='3'",1,1)
 SQL("airflow","select left(task_id, 2) td from  airflow.task_instance where dag_id = 'task_base_data_daily_new' group by td",1,1)

select dttm
       ,dag_id
       ,task_id
 from airflow.log
 where date_format(dttm, '%Y%m%d') = '20191026'
--- mysql 权限设置
use mysql;
select host, user from user;
-- update user set host = '%' where user = 'root'

grant all privileges on *.* to 'root'@'%' identified by 'Dwzq!951753';
flush privileges;


-- 查看时区
show variables like '%time_zone%';
-- 修改时区
set global time_zone = '+8:00';
set time_zone = '+8:00';
flush privileges;