# Databricks notebook source
# MAGIC %run /Repos/sebastian.rosengren@secotools.com/pipelines/mount_datalake

# COMMAND ----------

user <- "secoBI_Azure_admin"
password <- dbutils.secrets.get(scope = "azure_secrets", key = "oldsqlserverpassword")
url <- "jdbc:sqlserver://secobi-azure-test.database.windows.net:1433;database=Seco_Azure_Analytics_Test"

working_days <- spark_read_jdbc(sc,
                        name = "working_days",
                        options = list(user = user,
                        password = password,
                        url = url,
                        dbtable = "dbo.WorkingDays")) %>%
                 mutate(DateMonth = paste0(PeriodKey, "01")) %>%
                 mutate(DateMonth = from_unixtime(UNIX_TIMESTAMP(DateMonth, 'yyyyMMdd')))

# COMMAND ----------

working_days