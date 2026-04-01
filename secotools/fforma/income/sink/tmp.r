# Databricks notebook source
# MAGIC %run /Repos/secotools/pipelines/mount_datalake

# COMMAND ----------

# MAGIC %run /Repos/secotools/pipelines/income_read_productnumber

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
  rename(ProductNumber = ItemNumber)

# COMMAND ----------

product_basic %>% glimpse()

# COMMAND ----------

product_hist <- spark_read_jdbc(sc,
                        name = "product_hist",
                        options = list(user = user,
                        password = password,
                        url = url,
                        dbtable = "pub.view_ProductHistory")) 

# COMMAND ----------

product_basic %>% 
group_by(ProductNumber) %>%
  tally() %>%
  sdf_nrow()

# COMMAND ----------

product_hist %>% glimpse()

# COMMAND ----------

product_hist %>% 
  group_by(ItemNumber) %>%
  tally() %>%
  sdf_nrow()

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

# COMMAND ----------

product_basic <- spark_read_jdbc(sc,
                        name = "product_basic",
                        options = list(user = user,
                        password = password,
                        url = url,
                        dbtable = "pub.view_ProductBasic")) %>%
  rename(ProductNumber = ItemNumber)

# COMMAND ----------

product_basic %>% sdf_num_partitions

# COMMAND ----------

user <- "secoBI_Azure_admin"
password <- dbutils.secrets.get(scope = "azure_secrets", key = "oldsqlserverpassword")

# COMMAND ----------

url <- "jdbc:sqlserver://secobi-azure-test.database.windows.net:1433;database=Seco_Azure_Analytics_Test"
hl <- spark_read_jdbc(sc,
                        name = "hl",
                        options = list(user = user,
                        password = password,
                        url = url,
                        dbtable = "dbo.ForecastClassItem")) %>%
  rename(ProductNumber = ItemNumber)

# COMMAND ----------

hl_r <- hl %>% collect()

# COMMAND ----------

hl_r

# COMMAND ----------

path_out <- paste0("/mnt/blob/utils/HLGroupings.csv")
spark_write_csv(hl, 
                path = path_out, 
                delimiter = ";", 
                mode = "overwrite")

# COMMAND ----------

path_in_current <- paste0("/mnt/blob/forecast/income/final/", "current", "/income_forecast.csv")

# COMMAND ----------

forecast <- spark_read_csv(sc,
                           path = path_in_current,
                           delimiter = ";",
                           columns = list(
                                Date = "character",
                                SupplyingWarehouse = "character",
                                SalesMarket = "character",
                                ProductNumber = "character",
                                TotalOrders = "double",
                                ProductLine = "character",
                                ProductGroupDescription = "character",
                                ProductAccountGroupDescription = "character",
                                InventoryClassCode = "character",
                                SupplyMethod = "character",
                                values = "double",
                                values_adj = "double"
                           )) %>%
  group_by(Date, SupplyingWarehouse, SalesMarket, ProductNumber) %>%
  summarise(TotalOrders = sum(TotalOrders),
            values = sum(values),
            values_adj = sum(values_adj)) %>%
  arrange(ProductNumber, SupplyingWarehouse, SalesMarket, Date)

# COMMAND ----------

path_out <- paste0("/mnt/blob/forecast/income/final/", "archive/tmp", "/income_forecast.csv")
spark_write_csv(forecast %>% sdf_repartition(1), 
                path = path_out, 
                delimiter = ";", 
                mode = "overwrite")

# COMMAND ----------

product_basic_r <- product_basic %>% collect()


