

-- hive
hdfs dfs -put -f /root/base_data/jars/udf_get_age.jar /user/hive/udf
CREATE  FUNCTION base_data.udf_get_age AS 'com.soochow.udf.GetAgeUdf' USING JAR 'hdfs:///user/hive/udf/udf_get_age.jar';


CREATE  FUNCTION default.udf_conv2rgts AS 'com.soochow.udf.TransferUdf' USING JAR 'hdfs:///user/hive/udf/udf_conv2rgts.jar';
-- spark
add jar /root/base_data/jars/udf_get_age.jar;
CREATE temporary FUNCTION  udf_get_age AS 'com.soochow.udf.GetAgeUdf';


-- example
select udf_get_age('20010908');
select udf_get_age('20010912', '110221000501224');
select udf_get_age('20010908', '332102198106200339');