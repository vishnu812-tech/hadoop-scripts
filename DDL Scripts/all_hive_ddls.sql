-- Hive DDL Export with CREATE DATABASE and CREATE TABLE/VIEW
-- Generated on: Mon Jul 14 10:50:51 AM UTC 2025

-- DATABASE: default
CREATE DATABASE IF NOT EXISTS `default`;
USE `default`;

-- DATABASE: employee
CREATE DATABASE IF NOT EXISTS `employee`;
USE `employee`;

-- Table: employee.salaryinfo
CREATE TABLE `salaryinfo`(
  `id` int, 
  `employeename` string, 
  `jobtitle` string, 
  `basepay` string, 
  `overtimepay` string, 
  `otherpay` string, 
  `totalpay` double, 
  `totalpaybenefits` double, 
  `year` int, 
  `agency` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
WITH SERDEPROPERTIES ( 
  'path'='file:/home/vishnu/Notebooks/spark_notebooks/salary_transformation/spark-warehouse/employee.db/salaryinfo') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'file:/home/vishnu/Notebooks/spark_notebooks/salary_transformation/spark-warehouse/employee.db/salaryinfo'
TBLPROPERTIES (
  'spark.sql.create.version'='3.3.4', 
  'spark.sql.sources.provider'='parquet', 
  'spark.sql.sources.schema'='{"type":"struct","fields":[{"name":"Id","type":"integer","nullable":true,"metadata":{}},{"name":"EmployeeName","type":"string","nullable":true,"metadata":{}},{"name":"JobTitle","type":"string","nullable":true,"metadata":{}},{"name":"BasePay","type":"string","nullable":true,"metadata":{}},{"name":"OvertimePay","type":"string","nullable":true,"metadata":{}},{"name":"OtherPay","type":"string","nullable":true,"metadata":{}},{"name":"TotalPay","type":"double","nullable":true,"metadata":{}},{"name":"TotalPayBenefits","type":"double","nullable":true,"metadata":{}},{"name":"Year","type":"integer","nullable":true,"metadata":{}},{"name":"Agency","type":"string","nullable":true,"metadata":{}}]}', 
  'transient_lastDdlTime'='1752238509')



-- DATABASE: iot
CREATE DATABASE IF NOT EXISTS `iot`;
USE `iot`;

-- Table: iot.stress_data
CREATE TABLE `stress_data`(
  `date_time` string, 
  `userid` int, 
  `username` string, 
  `activity` string, 
  `ax` double, 
  `ay` double, 
  `az` double, 
  `gx` double, 
  `gy` double, 
  `gz` double, 
  `mx` double, 
  `my` double, 
  `mz` double)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
WITH SERDEPROPERTIES ( 
  'path'='file:/home/vishnu/Notebooks/spark-warehouse/iot.db/stress_data') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'file:/home/vishnu/Notebooks/spark-warehouse/iot.db/stress_data'
TBLPROPERTIES (
  'spark.sql.create.version'='3.3.4', 
  'spark.sql.sources.provider'='parquet', 
  'spark.sql.sources.schema'='{"type":"struct","fields":[{"name":"date_time","type":"string","nullable":true,"metadata":{}},{"name":"userid","type":"integer","nullable":true,"metadata":{}},{"name":"username","type":"string","nullable":true,"metadata":{}},{"name":"Activity","type":"string","nullable":true,"metadata":{}},{"name":"Ax","type":"double","nullable":true,"metadata":{}},{"name":"Ay","type":"double","nullable":true,"metadata":{}},{"name":"Az","type":"double","nullable":true,"metadata":{}},{"name":"Gx","type":"double","nullable":true,"metadata":{}},{"name":"Gy","type":"double","nullable":true,"metadata":{}},{"name":"Gz","type":"double","nullable":true,"metadata":{}},{"name":"Mx","type":"double","nullable":true,"metadata":{}},{"name":"My","type":"double","nullable":true,"metadata":{}},{"name":"Mz","type":"double","nullable":true,"metadata":{}}]}', 
  'transient_lastDdlTime'='1752132747')


