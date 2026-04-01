# Databricks notebook source
# MAGIC %run /Repos/alejandro.kuratomi_hernandez@secotools.com/prophet_R_secotools/secotools/pipelines/mount_datalake

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

path_in <- paste0("/mnt/blob/forecast/income/final/", "current", "/income_forecast.csv")
az_forecast <- spark_read_csv(sc, 
                       path = path_in,
                       delimiter = ";",
                       columns = list(
                         Date = "character",
                         SupplyingWarehouse = "character",
                         SalesMarket = "character",
                         ProductNumber = "character",
                         TotalOrders = "double",
                         ProductLine = "character",
                         values = "double",
                         values_adj = "double")) %>%
               select(Date, 
                     SupplyingWarehouse, 
                     SalesMarket,
                     ProductNumber,
                     TotalOrders,
                     values,
                     values_adj)


