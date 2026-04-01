# Databricks notebook source
# MAGIC %run /Repos/secotools/pipelines/mount_datalake

# COMMAND ----------

user <- dbutils.secrets.get(scope = "azure_secrets", key = "sqluser")
password <- dbutils.secrets.get(scope = "azure_secrets", key = "sqlpassword")
url <- "jdbc:sqlserver://seco-azure-info-prod.database.windows.net:1433;database=seco-azure-info-prod"

product_basic <- spark_read_jdbc(sc,
                        name = "product_basic",
                        options = list(user = user,
                        password = password,
                        url = url,
                        dbtable = "pub.view_ProductBasic")) %>%
  rename(ProductNumber = ItemNumber) %>%
  select(ProductNumber, 
         ProductLine,
         ProductAccountGroup,
         ProductGroupCode,
         Hierarchy2,
         InventoryClassCode,
         Grade,
         Designation,
         SupplyMethod,
         MainSupplier,
         ProductManagementArea,
         ProductManagementArea2) %>%
  mutate(Designation = regexp_replace(Designation, "\"", ""))

# COMMAND ----------

path_out <- paste0("/mnt/blob/utils/product_basic.csv")
spark_write_csv(product_basic, 
                path = path_out, 
                delimiter = ",", 
                mode = "overwrite")


