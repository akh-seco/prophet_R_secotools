# Databricks notebook source
library(tidyverse)
library(sparklyr)
sc <- spark_connect(method = "databricks")

# COMMAND ----------



# COMMAND ----------

user <- dbutils.secrets.get(scope = "azure_secrets", key = "sqluser")
password <- dbutils.secrets.get(scope = "azure_secrets", key = "sqlpassword")

# COMMAND ----------

url <- "jdbc:sqlserver://seco-azure-info-prod.database.windows.net:1433;database=seco-azure-info-prod"
income <- spark_read_jdbc(sc,
                        name = "income",
                        options = list(user = user,
                        password = password,
                        url = url,
                        dbtable = "pub.view_DCOrderIncome"))