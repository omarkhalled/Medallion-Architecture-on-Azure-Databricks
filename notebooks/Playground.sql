-- Databricks notebook source
-- Count of Bronze Traffic Rows

SELECT COUNT(*) FROM `dev_catalog`.`bronze`.raw_traffic

-- COMMAND ----------

-- Count of Bronze Roads Rows

SELECT COUNT(*) FROM `dev_catalog`.`bronze`.raw_roads

-- COMMAND ----------

-- Count of Silver Traffic Rows

SELECT COUNT(*) FROM `dev_catalog`.`silver`.silver_traffic

-- COMMAND ----------

-- Count of Silver Roads Rows

SELECT COUNT(*) FROM `dev_catalog`.`silver`.silver_roads

-- COMMAND ----------

-- Count of Gold Traffic Rows

SELECT COUNT(*) FROM `dev_catalog`.`gold`.gold_traffic

-- COMMAND ----------

-- Count of Gold Road Rows

SELECT COUNT(*) FROM `dev_catalog`.`gold`.gold_roads

-- COMMAND ----------


