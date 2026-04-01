# Databricks notebook source
# MAGIC %run /Repos/secotools/pipelines/mount_datalake

# COMMAND ----------

user <- dbutils.secrets.get(scope = "azure_secrets", key = "sqluser")
password <- dbutils.secrets.get(scope = "azure_secrets", key = "sqlpassword")

url <- "jdbc:sqlserver://seco-azure-info-prod.database.windows.net:1433;database=seco-azure-info-prod"
ProductHistory <- spark_read_jdbc(sc,
                        name = "ProductHistory",
                        options = list(user = user,
                        password = password,
                        url = url,
                        dbtable = "pub.view_ProductHistory")) %>%
  rename(ProductNumber = ItemNumber) %>%
  mutate(StartDateDay = StartDate,
         EndDateDay = EndDate) %>%
  mutate(diffday = datediff(EndDateDay, StartDateDay)) %>%
  mutate(StartDate = paste0(StartDate %>% substr(1, 4), "-", StartDate %>% substr(6, 7), "-01") %>% to_date("yyyy-MM-dd"),
         EndDate = paste0(EndDate %>% substr(1, 4), "-", EndDate %>% substr(6, 7), "-01") %>% to_date("yyyy-MM-dd")) %>%
  mutate(diff = datediff(EndDate, StartDate)) %>%
  sdf_register("ProductHistory")

tbl_cache(sc, "ProductHistory")

# COMMAND ----------

tmp <- ProductHistory %>% 
  select(ProductNumber, StartDate, EndDate, StartDateDay, EndDateDay) %>%
  group_by(ProductNumber) %>%
  mutate(n = n()) %>%
  ungroup() %>%
  filter(n > 175) %>%
  collect()

# COMMAND ----------

tmp


