# Databricks notebook source
# MAGIC %run /Repos/sebastian.rosengren@secotools.com/pipelines/mount_datalake

# COMMAND ----------

user <- dbutils.secrets.get(scope = "azure_secrets", key = "sqluser")
password <- dbutils.secrets.get(scope = "azure_secrets", key = "sqlpassword")

url <- "jdbc:sqlserver://seco-azure-info-prod.database.windows.net:1433;database=seco-azure-info-prod"
ProjectID <- spark_read_jdbc(sc,
                        name = "ProjectID",
                        options = list(user = user,
                        password = password,
                        url = url,
                        dbtable = "pub.view_ProductExtended")) %>%
  rename(ProductNumber = ItemNumber) %>%
  select(ProductNumber, ProjectID)

# COMMAND ----------

ProductForecastSeries_raw %>% pull(ForecastPeriod) %>% min()

# COMMAND ----------

user <- dbutils.secrets.get(scope = "azure_secrets", key = "sqluser")
password <- dbutils.secrets.get(scope = "azure_secrets", key = "sqlpassword")

url <- "jdbc:sqlserver://seco-azure-info-prod.database.windows.net:1433;database=seco-azure-info-prod"
ProductForecastSeries_raw <- spark_read_jdbc(sc,
                        name = "ProductForecastSeries",
                        options = list(user = user,
                        password = password,
                        url = url,
                        dbtable = "pub.view_ProductForecastSeries")) %>%
   rename(res_forecast = FCST_RSLT_QTY,
          stat_forecast = FCST_SYS_QTY,
          Demand = DMD_ACTL_QTY) %>%
   mutate(Date = paste0(FCST_YR_PRD %>% substr(1, 4), "-", FCST_YR_PRD %>% substr(5, 6), "-01"))
  

end_period <- ProductForecastSeries_raw %>% pull(ForecastPeriod) %>% max()

ProductForecastSeries <- ProductForecastSeries_raw %>%
  filter(FCST_YR_PRD == ForecastPeriod | 
         ((ForecastPeriod == end_period) & (FCST_YR_PRD > ForecastPeriod)) |
         (ForecastPeriod == 202201 & FCST_YR_PRD == 202202) ) %>%
  select(ForecastPeriod, 
         SCP_SEQ_NBR, 
         Date, 
         res_forecast,
         stat_forecast,
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
  filter(LVL_NBR == 2) %>%
  rename(ProductNumber = FCST_1_ID,
         SupplyingWarehouse = FCST_2_ID,
         SalesMarket = FCST_3_ID) %>%
  mutate(SupplyingWarehouse = if_else( SupplyingWarehouse == " ", "MISSING", SupplyingWarehouse )) %>%
  mutate(SupplyingWarehouse = if_else( SupplyingWarehouse == "EDC", "DCE", SupplyingWarehouse )) %>%
  #mutate(SalesMarket = if_else( SalesMarket == " ", "MISSING", SalesMarket )) %>%
  #mutate(SalesMarket = if_else( SalesMarket == "CA", "US", SalesMarket )) %>%
  select(ForecastPeriod, 
         SupplyingWarehouse, 
         #SalesMarket, 
         ProductNumber, 
         SCP_SEQ_NBR, 
         LVL_NBR,
         USR_10_TEXT,
         USR_24_TEXT)

# COMMAND ----------

voy_forecast <- ProductForecastSeries %>%
  left_join(ProductForecastRoot) %>%
  filter(LVL_NBR == 2) %>%
  select(Date, 
         SupplyingWarehouse, 
         SCP_SEQ_NBR,
         ProductNumber, 
         res_forecast,
         stat_forecast,
         USR_06_QTY,
         USR_07_QTY,
         USR_08_QTY) %>% 
  #mutate(SalesMarket = if_else(SalesMarket == "CA", "US", SalesMarket)) %>%
  group_by(Date, 
           SupplyingWarehouse, 
           SCP_SEQ_NBR,
           ProductNumber,
           USR_06_QTY,
           USR_07_QTY,
           USR_08_QTY) %>%
  summarise(res_forecast = sum(res_forecast),
            stat_forecast = sum(stat_forecast)) %>%
  ungroup() %>%
  sdf_register("voy_forecast")

tbl_cache(sc, "voy_forecast")

# COMMAND ----------

voy_forecast %>%
  group_by(Date) %>%
  summarise(y = sum(res_forecast),
            stat = sum(stat_forecast)) %>%
  collect() %>%
  mutate(Date = Date %>% as.Date()) %>%
  ggplot()+
  geom_line(aes(x = Date, y = y))+
  geom_line(aes(x = Date, y = stat))

# COMMAND ----------

##Get latest logged data
voy_demand <- ProductForecastSeries_raw %>%
  left_join(ProductForecastRoot) %>%
  filter(LVL_NBR == 2, FCST_YR_PRD >= min(ForecastPeriod)) %>%
  group_by(Date, 
           SupplyingWarehouse, 
           SCP_SEQ_NBR,
           ProductNumber) %>%
  filter(ForecastPeriod == max(ForecastPeriod)) %>%
  summarise(Demand = sum(Demand)) %>%
  ungroup() %>%
  sdf_register("voy_demand")


tbl_cache(sc, "voy_demand")

# COMMAND ----------

ProductForecastSeries_raw %>%
  left_join(ProductForecastRoot) %>% glimpse()

# COMMAND ----------

if(FALSE){
voy_demand <- ProductForecastSeries_raw %>%
  left_join(ProductForecastRoot) %>%
  filter(LVL_NBR == 2) %>%
  group_by(SupplyingWarehouse, 
           SCP_SEQ_NBR, 
           ProductNumber, 
           ForecastPeriod) %>%
  filter( ((ForecastPeriod - FCST_YR_PRD) == 1) )  %>%
  ungroup() %>% 
  group_by(Date, 
           SupplyingWarehouse, 
           SCP_SEQ_NBR, 
           ProductNumber) %>%
  summarise(Demand = sum(Demand)) %>%
  ungroup() %>%
  sdf_register("voy_demand")

tbl_cache(sc, "voy_demand")
}

# COMMAND ----------

Date <- voy_demand %>% pull(Date) %>% unique()
SupplyingWarehouse <- voy_demand %>% pull(SupplyingWarehouse) %>% unique()
ProductNumber <- voy_demand %>% pull(ProductNumber) %>% unique()
SCP_SEQ_NBR <- -1

tmp <- sdf_expand_grid(sc, 
                       Date = Date, 
                       SupplyingWarehouse = SupplyingWarehouse, 
                       ProductNumber = ProductNumber)

##Needed because TotalOrders and Demand might be logged on different DC:s
##This makes aggregate in PBI correct

# COMMAND ----------

voy_demand <- tmp %>%
  left_join(voy_demand)

# COMMAND ----------

voy_demand %>% 
  group_by(Date) %>%
  summarise(y = sum(Demand)) %>%
  arrange(Date) %>%
  print(n = 200)

# COMMAND ----------

voy_forecast %>%
  group_by(Date) %>%
  summarise(y = sum(res_forecast),
            stat = sum(stat_forecast)) %>%
  arrange(Date) %>%
  print(n = 40)

# COMMAND ----------

voy_demand %>% 
  group_by(Date) %>%
  summarise(y = sum(Demand)) %>%
  collect() %>%
  mutate(Date = Date %>% as.Date()) %>%
  ggplot()+
  geom_line(aes(x = Date, y = y))

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
                         TransactionDate = "integer",
                         OrderDate = "integer",
                         Quantity = "double")) %>%
            rename(ProductNumber = ItemNumber,
                   SupplyingWarehouse = Warehouse) %>%
            mutate(Date = paste0(TransactionDate %>% substr(1, 4), "-", TransactionDate %>% substr(5, 6), "-01"),
                   SalesMarket = if_else(SalesMarket == "CA", "US", SalesMarket),
                   SalesMarket = if_else(SalesMarket %>% is.na(), "MISSING", SalesMarket),
                   SupplyingWarehouse = if_else(SupplyingWarehouse == "EDC", "DCE", SupplyingWarehouse)) %>%
            mutate(dm = Date %>% to_date("yyyy-MM-dd")) %>%
            filter(dm <= end_date) %>%
            #group_by(Date, SupplyingWarehouse, SalesMarket, ProductNumber)  %>%
            group_by(Date, SupplyingWarehouse, ProductNumber) %>%
            summarise(TotalOrders = sum(Quantity)) %>%
            ungroup()

# COMMAND ----------

voy_data <- voy_forecast %>%
  full_join(voy_demand) %>%
  left_join(income_pn_dc_sm) %>%
  #full_join(income_pn_dc_sm) %>%
  mutate(Demand = if_else(Demand %>% is.na(), 0, Demand),
         res_forecast = if_else(res_forecast %>% is.na(), 0, res_forecast),
         stat_forecast = if_else(stat_forecast %>% is.na(), 0, stat_forecast),
         TotalOrders = if_else(TotalOrders %>% is.na(), 0, TotalOrders),
         has_acc = if_else(Date <= end_date, TRUE, FALSE)) %>%
  left_join(product_basic) %>%
  left_join(ProjectID) %>%
  left_join(ProductForecastRoot %>%
            mutate(Date = paste0(ForecastPeriod %>% substr(1, 4), "-", ForecastPeriod %>% substr(5, 6), "-01")) %>%
             group_by(Date, 
                      SCP_SEQ_NBR,
                      USR_10_TEXT, 
                      USR_24_TEXT) %>%
             tally() %>%
             ungroup() %>%
             select(-n))

# COMMAND ----------

voy_data %>% 
  group_by(Date) %>%
  summarise(Demand = sum(Demand),
            TotalOrders = sum(TotalOrders),
            Forecast = sum(res_forecast),
            statistical = sum(stat_forecast)) %>%
  #mutate(mape = 100 * abs(TotalOrders - Forecast) / TotalOrders) %>%
  arrange(Date) %>%
  print(n = 100)

# COMMAND ----------

mape <- voy_data %>% 
  group_by(Date) %>%
  summarise(Demand = sum(Demand),
            TotalOrders = sum(TotalOrders),
            Forecast = sum(res_forecast),
            statistical = sum(stat_forecast)) %>%
  #mutate(mape = 100 * abs(TotalOrders - Forecast) / TotalOrders) %>%
  mutate(Forecast = abs(Forecast - TotalOrders) / TotalOrders, 
            statistical = abs(statistical - TotalOrders) / TotalOrders) %>%
  arrange(Date) %>%
  collect() %>%
  filter(complete.cases(.))

# COMMAND ----------

mape

# COMMAND ----------

voy_data %>% glimpse()

# COMMAND ----------

path_out_archive <- paste0("/mnt/blob/pbi/income/archive/pbi_forecast_accuracy")
path_out_current <- paste0("/mnt/blob/pbi/income/forecast_accuracy", "/pbi_forecast_accuracy")

dbutils.fs.rm(path_out_archive, TRUE)
dbutils.fs.rm(path_out_current, TRUE)

spark_write_csv(voy_data %>% sdf_repartition(1), 
                path = path_out_archive, 
                delimiter = ";", 
                mode = "overwrite")

file_list <- dbutils.fs.ls(path_out_archive)
for(lst in file_list) {
  if(lst$size < 1000) dbutils.fs.rm(lst$path, TRUE)
  else{
    dbutils.fs.cp(lst$path, path_out_current)
  }
}