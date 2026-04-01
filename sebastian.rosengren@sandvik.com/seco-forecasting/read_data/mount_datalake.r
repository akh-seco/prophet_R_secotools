# Databricks notebook source
# MAGIC %python
# MAGIC #if not any(mount.mountPoint == '/mnt/blob/data' for mount in dbutils.fs.mounts()):
# MAGIC #  dbutils.fs.mount(
# MAGIC #    source = "wasbs://data@analyticsdatalakeprodwe.blob.core.windows.net",
# MAGIC #    mount_point = "/mnt/blob/data",
# MAGIC #    extra_configs = {"fs.azure.account.key.analyticsdatalakeprodwe.blob.core.windows.net":dbutils.secrets.get(scope = "azure_secrets", key = "blobpassword")})
# MAGIC   
# MAGIC if not any(mount.mountPoint == '/mnt/blob/data' for mount in dbutils.fs.mounts()):
# MAGIC   dbutils.fs.mount(
# MAGIC     source = "wasbs://data@analyticsforecastprodwe.blob.core.windows.net",
# MAGIC     mount_point = "/mnt/blob/data",
# MAGIC     extra_configs = {"fs.azure.account.key.analyticsforecastprodwe.blob.core.windows.net":dbutils.secrets.get(scope = "azure_secrets", key = "blobpassword")}) 
# MAGIC
# MAGIC #Ask for access to key vault to update keys or seb count generate keys
# MAGIC if not any(mount.mountPoint == '/mnt/blob/utils' for mount in dbutils.fs.mounts()):
# MAGIC   dbutils.fs.mount(
# MAGIC     source = "wasbs://utils@analyticsdatalakeprodwe.blob.core.windows.net",
# MAGIC     mount_point = "/mnt/blob/utils",
# MAGIC     extra_configs = {"fs.azure.account.key.analyticsdatalakeprodwe.blob.core.windows.net":dbutils.secrets.get(scope = "azure_secrets", key = "blobpassword")}) 
# MAGIC   
# MAGIC if not any(mount.mountPoint == '/mnt/blob/hyperparameters' for mount in dbutils.fs.mounts()):
# MAGIC   dbutils.fs.mount(
# MAGIC     source = "wasbs://hyperparameters@analyticsdatalakeprodwe.blob.core.windows.net",
# MAGIC     mount_point = "/mnt/blob/hyperparameters",
# MAGIC     extra_configs = {"fs.azure.account.key.analyticsdatalakeprodwe.blob.core.windows.net":dbutils.secrets.get(scope = "azure_secrets", key = "blobpassword")})    
# MAGIC  
# MAGIC if not any(mount.mountPoint == '/mnt/blob/forecast' for mount in dbutils.fs.mounts()):
# MAGIC   dbutils.fs.mount(
# MAGIC     source = "wasbs://forecast@analyticsdatalakeprodwe.blob.core.windows.net",
# MAGIC     mount_point = "/mnt/blob/forecast",
# MAGIC     extra_configs = {"fs.azure.account.key.analyticsdatalakeprodwe.blob.core.windows.net":dbutils.secrets.get(scope = "azure_secrets", key = "blobpassword")})    
# MAGIC   
# MAGIC if not any(mount.mountPoint == '/mnt/blob/pbi' for mount in dbutils.fs.mounts()):
# MAGIC   dbutils.fs.mount(
# MAGIC     source = "wasbs://pbi@analyticsdatalakeprodwe.blob.core.windows.net",
# MAGIC     mount_point = "/mnt/blob/pbi",
# MAGIC     extra_configs = {"fs.azure.account.key.analyticsdatalakeprodwe.blob.core.windows.net":dbutils.secrets.get(scope = "azure_secrets", key = "blobpassword")})  

# COMMAND ----------

library(tidyverse)
library(sparklyr)
sc <- spark_connect(method = "databricks")

# COMMAND ----------

blob_files <- dbutils.fs.ls("/mnt/blob/data/dc_orderincome")
files <- NULL
for(lst in blob_files) files <- c(files, lst$name)
dc_order_latest <- files %>% sort() %>% tail(1)
print(paste0("Latest DC Order Income: ", dc_order_latest))

blob_files <- dbutils.fs.ls("/mnt/blob/data/co_orderinvoice")
files <- NULL
for(lst in blob_files) files <- c(files, lst$name)
co_order_latest <- files %>% sort() %>% tail(1)
print(paste0("Latest CO Order Invoice: ", co_order_latest))

blob_files <- dbutils.fs.ls("/mnt/blob/data/iw_dimvproduct")
files <- NULL
for(lst in blob_files) files <- c(files, lst$name)
iw_dimvproduct_latest <- files %>% sort() %>% tail(1)
print(paste0("Latest iw_dimvproduct: ", iw_dimvproduct_latest))

# COMMAND ----------

library(lubridate)
formatted <- gsub("T00:00:00|-", "", dc_order_latest)
end_date <- str_split(formatted, "_")[[1]][3] %>% 
  substr(1,6) %>%
  paste0("01") %>%
  as.Date(format = "%Y%m%d") %m-% months(1)

# COMMAND ----------

end_date

# COMMAND ----------

sc %>% spark_session() %>% invoke("catalog") %>% 
  invoke("clearCache")