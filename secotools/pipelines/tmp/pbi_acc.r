# Databricks notebook source
# MAGIC %run /Repos/secotools/pipelines/mount_datalake

# COMMAND ----------

user <- dbutils.secrets.get(scope = "azure_secrets", key = "sqluser")
password <- dbutils.secrets.get(scope = "azure_secrets", key = "sqlpassword")

url <- "jdbc:sqlserver://seco-azure-info-prod.database.windows.net:1433;database=seco-azure-info-prod"
ProductForecastSeries <- spark_read_jdbc(sc,
                        name = "ProductForecastSeries",
                        options = list(user = user,
                        password = password,
                        url = url,
                        dbtable = "pub.view_ProductForecastSeries")) 

end_period <- ProductForecastSeries %>% pull(ForecastPeriod) %>% max()

ProductForecastSeries <- ProductForecastSeries %>%
  filter(ForecastPeriod == end_period, FCST_YR_PRD >= ForecastPeriod) %>%
  rename(Date = FCST_YR_PRD,
         res_forecast = FCST_RSLT_QTY) %>%
  mutate(Date = paste0(Date %>% substr(1, 4), "-", Date %>% substr(5, 6), "-01")) %>%
  select(ForecastPeriod, 
         SCP_SEQ_NBR, Date, 
         res_forecast)

# COMMAND ----------

user <- dbutils.secrets.get(scope = "azure_secrets", key = "sqluser")
password <- dbutils.secrets.get(scope = "azure_secrets", key = "sqlpassword")

url <- "jdbc:sqlserver://seco-azure-info-prod.database.windows.net:1433;database=seco-azure-info-prod"
ProductForecastRoot <- spark_read_jdbc(sc,
                        name = "ProductForecastRoot",
                        options = list(user = user,
                        password = password,
                        url = url,
                        dbtable = "pub.view_ProductForecastRoot")) %>%
  filter(ForecastPeriod == end_period, LVL_NBR == 1) %>%
  rename(ProductNumber = FCST_1_ID,
         SupplyingWarehouse = FCST_2_ID,
         SalesMarket = FCST_3_ID) %>%
  mutate(SupplyingWarehouse = if_else( SupplyingWarehouse == " ", "MISSING", SupplyingWarehouse )) %>%
  mutate(SupplyingWarehouse = if_else( SupplyingWarehouse == "EDC", "DCE", SupplyingWarehouse )) %>%
  mutate(SalesMarket = if_else( SalesMarket == " ", "MISSING", SalesMarket )) %>%
  mutate(SalesMarket = if_else( SalesMarket == "CA", "US", SalesMarket )) %>%
  select(ForecastPeriod, 
         SupplyingWarehouse, 
         SalesMarket, 
         ProductNumber, 
         SCP_SEQ_NBR, 
         LVL_NBR) %>%
  distinct()

# COMMAND ----------

voy_data <- ProductForecastSeries %>%
  left_join(ProductForecastRoot) %>%
  filter(LVL_NBR == 1,
         Date > end_date) %>%
  select(Date, 
         SupplyingWarehouse, 
         SalesMarket, 
         ProductNumber, 
         res_forecast) %>% 
  group_by(Date, 
           SupplyingWarehouse, 
           SalesMarket, 
           ProductNumber) %>%
  summarise(res_forecast = sum(res_forecast)) %>%
  ungroup() %>%
  sdf_register("voy_data")

tbl_cache(sc, "voy_data")

# COMMAND ----------

has_voy_forecast <- voy_data %>%
  #group_by(ProductNumber, SupplyingWarehouse, SalesMarket) %>% #"Ja det Ã¤r ett problem vi har att sales unit som anvÃ¤nds i voyager inte finns exakt lika nÃ¥gon annanstans utan dÃ¥ Ã¤r det ungefÃ¤rligt lika med sÃ¤ljande land"
  group_by(ProductNumber) %>%
  summarise(has_voy_forecast = TRUE) %>%
  ungroup()

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

# COMMAND ----------

has_az_forecast <- az_forecast %>%
  group_by(ProductNumber, SupplyingWarehouse, SalesMarket) %>%
  summarise(has_az_forecast = TRUE) %>%
  ungroup()

# COMMAND ----------

all_data <- az_forecast %>%
  full_join(voy_data) %>%
  left_join(has_az_forecast) %>%
  left_join(has_voy_forecast) %>%
  left_join(product_basic) %>%
  mutate(has_az_forecast = if_else(has_az_forecast %>% is.na(), FALSE, has_az_forecast),
         has_voy_forecast = if_else(has_voy_forecast %>% is.na(), FALSE, has_voy_forecast)) %>%
  mutate(ProductLine = if_else(ProductLine %>% is.na(), "MISSING", ProductLine)) %>%
  mutate(has_hierachy = if_else(Hierarchy2 %>% is.na(), FALSE, TRUE)) %>% 
  mutate(TotalOrders_Forecast = values_adj) %>% 
  mutate(TotalOrders_Forecast = if_else(is.na(TotalOrders_Forecast), TotalOrders, TotalOrders_Forecast)) %>%
  sdf_register("all_data")

tbl_cache(sc, "all_data")

# COMMAND ----------

user <- "secoBI_Azure_admin"
password <- dbutils.secrets.get(scope = "azure_secrets", key = "oldsqlserverpassword")
url <- "jdbc:sqlserver://secobi-azure-test.database.windows.net:1433;database=Seco_Azure_Analytics_Test"

HLGroupings <- spark_read_jdbc(sc,
                        name = "HLGroupings",
                        options = list(user = user,
                        password = password,
                        url = url,
                        dbtable = "dbo.ForecastClassItem")) %>%
                rename(ProductNumber = ItemNumber)

# COMMAND ----------

user <- dbutils.secrets.get(scope = "azure_secrets", key = "sqluser")
password <- dbutils.secrets.get(scope = "azure_secrets", key = "sqlpassword")

url <- "jdbc:sqlserver://seco-azure-info-prod.database.windows.net:1433;database=seco-azure-info-prod"
ProductForecastSeries <- spark_read_jdbc(sc,
                        name = "ProductForecastSeries",
                        options = list(user = user,
                        password = password,
                        url = url,
                        dbtable = "pub.view_ProductForecastSeries"))

# COMMAND ----------

ProductForecastSeries %>% glimpse()

# COMMAND ----------

user <- dbutils.secrets.get(scope = "azure_secrets", key = "sqluser")
password <- dbutils.secrets.get(scope = "azure_secrets", key = "sqlpassword")

url <- "jdbc:sqlserver://seco-azure-info-prod.database.windows.net:1433;database=seco-azure-info-prod"
ProductForecastRoot <- spark_read_jdbc(sc,
                        name = "ProductForecastRoot",
                        options = list(user = user,
                        password = password,
                        url = url,
                        dbtable = "pub.view_ProductForecastRoot"))

# COMMAND ----------

ProductForecastRoot %>% glimpse()


