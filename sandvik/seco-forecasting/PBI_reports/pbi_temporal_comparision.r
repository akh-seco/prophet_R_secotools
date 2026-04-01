# Databricks notebook source
# MAGIC %run /Repos/alejandro.kuratomi_hernandez@secotools.com/prophet_R_secotools/sandvik/seco-forecasting/read_data/mount_datalake

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
         SCP_SEQ_NBR, 
         Date, 
         res_forecast,
         USR_06_QTY,
         USR_07_QTY,
         USR_08_QTY)

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
         LVL_NBR,
         USR_10_TEXT,
         USR_24_TEXT) %>%
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
         res_forecast,
         USR_06_QTY,
         USR_07_QTY,
         USR_08_QTY) %>% 
  mutate(SalesMarket = if_else(SalesMarket == "CA", "US", SalesMarket)) %>%
  group_by(Date, 
           SupplyingWarehouse, 
           SalesMarket, 
           ProductNumber,
           USR_06_QTY,
           USR_07_QTY,
           USR_08_QTY) %>%
  summarise(res_forecast = sum(res_forecast)) %>%
  ungroup() %>%
  sdf_register("voy_data")

tbl_cache(sc, "voy_data")

# COMMAND ----------

has_voy_forecast <- voy_data %>%
  #group_by(ProductNumber, SupplyingWarehouse, SalesMarket) %>% #"Ja det är ett problem vi har att sales unit som används i voyager inte finns exakt lika någon annanstans utan då är det ungefärligt lika med säljande land"
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
         ProductManagementArea2,
         ForecastingGroup,
         PlannedReleaseDate,
         PlannedDeletionDate) %>%
  mutate(Designation = regexp_replace(Designation, "\"", ""))

# COMMAND ----------

path_in <- paste0("/mnt/blob/data/dc_orderincome/", dc_order_latest) 
income_pn_dc_sm <- spark_read_csv(sc, 
                       path = path_in,
                       delimiter = ";",
                       columns = list(
                         SalesMarket = "character",
                         Warehouse = "character",
                         ItemNumber = "character",
                         TransactionDate = "character",
                         OrderDate = "integer",
                         Quantity = "double")) %>%
            rename(ProductNumber = ItemNumber,
                   SupplyingWarehouse = Warehouse) %>%
            mutate(Date = paste0(regexp_replace(TransactionDate, "-", "") %>% substr(1, 4), "-", regexp_replace(TransactionDate, "-", "") %>% substr(5, 6), "-01"),
                   SalesMarket = if_else(SalesMarket == "CA", "US", SalesMarket),
                   SalesMarket = if_else(SalesMarket %>% is.na(), "MISSING", SalesMarket),
                   SupplyingWarehouse = if_else(SupplyingWarehouse == "EDC", "DCE", SupplyingWarehouse)) %>%
            mutate(dm = Date %>% to_date("yyyy-MM-dd")) %>%
            filter(dm <= end_date) %>%
            group_by(Date, SupplyingWarehouse, SalesMarket, ProductNumber)  %>%
            #group_by(Date, SupplyingWarehouse, ProductNumber) %>%
            summarise(TotalOrders = sum(Quantity)) %>%
            ungroup()

# COMMAND ----------

#Check USR_10_24 with SCP_SEQ_NBR

# COMMAND ----------

pbi_data <- voy_data %>%
  full_join(income_pn_dc_sm) %>%
  left_join(has_voy_forecast) %>%
  filter(has_voy_forecast) %>%
  mutate(h = months_between(Date %>% to_date(), end_date)) %>%
  mutate(quarter = (h >= -2 & h <= 3) %>% as.numeric(),
         half_year = (h >= -5 & h <= 6) %>% as.numeric(),
         year = (h >= -11 & h <= 12) %>% as.numeric()) %>%
  left_join(product_basic) %>%
  left_join(ProductForecastRoot %>%
             group_by(SupplyingWarehouse, 
                      SalesMarket, 
                      ProductNumber,
                      USR_10_TEXT, 
                      USR_24_TEXT) %>%
             tally() %>%
             ungroup() %>%
             select(-n)) %>%
  sdf_register("pbi_data")

tbl_cache(sc, "pbi_data")

# COMMAND ----------

  pbi_data %>% glimpse()

# COMMAND ----------

path_out_archive <- paste0("/mnt/blob/pbi/income/archive/temporal_cf")
path_out_current <- paste0("/mnt/blob/pbi/income/temporal_cf", "/pbi_temporal_cf.csv")

dbutils.fs.rm(path_out_archive, TRUE)
dbutils.fs.rm(path_out_current, TRUE)

spark_write_csv(pbi_data %>% sdf_repartition(1), 
                path = path_out_archive, 
                delimiter = ";", 
                mode = "overwrite")

file_list <- dbutils.fs.ls(path_out_archive)
for(lst in file_list) {
  if(lst$size < 1000) dbutils.fs.rm(lst$path, TRUE)
  else{
    dbutils.fs.mv(lst$path, path_out_current)
  }
}
