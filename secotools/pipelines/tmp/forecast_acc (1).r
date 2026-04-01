# Databricks notebook source
# MAGIC %run /Repos/alejandro.kuratomi_hernandez@secotools.com/prophet_R_secotools/secotools/pipelines/mount_datalake

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
  filter(LVL_NBR == 3) %>%
  rename(ProductNumber = FCST_1_ID,
         #SupplyingWarehouse = FCST_2_ID,
         SalesMarket = FCST_3_ID) %>%
  #mutate(SupplyingWarehouse = if_else( SupplyingWarehouse == " ", "MISSING", SupplyingWarehouse )) %>% #Check this
  #mutate(SupplyingWarehouse = if_else( SupplyingWarehouse == "EDC", "DCE", SupplyingWarehouse )) %>%
  #mutate(SalesMarket = if_else( SalesMarket == " ", "MISSING", SalesMarket )) %>%
  #mutate(SalesMarket = if_else( SalesMarket == "CA", "US", SalesMarket )) %>%
  select(ForecastPeriod, 
         #SalesMarket, 
         ProductNumber, 
         SCP_SEQ_NBR, 
         LVL_NBR,
         USR_10_TEXT,
         USR_24_TEXT)

# COMMAND ----------

ProductForecastSeries_raw %>%
  left_join(ProductForecastRoot) %>%
  glimpse()

# COMMAND ----------

voy_forecast <- ProductForecastSeries %>%
  left_join(ProductForecastRoot) %>%
  filter(LVL_NBR == 3) %>%
  select(Date, 
         ProductNumber, 
         res_forecast,
         stat_forecast,
         USR_06_QTY,
         USR_07_QTY,
         USR_08_QTY) %>% 
  #mutate(SalesMarket = if_else(SalesMarket == "CA", "US", SalesMarket)) %>%
  group_by(Date,
           #SalesMarket, 
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

voy_demand <- ProductForecastSeries_raw %>%
  left_join(ProductForecastRoot) %>%
  filter(LVL_NBR == 3) %>%
  filter( ForecastPeriod == 202205, FCST_YR_PRD < ForecastPeriod  )  %>%
  group_by( 
           #SalesMarket, 
           ProductNumber, 
           ForecastPeriod) %>%
  ungroup() %>% 
  group_by(Date,        
           #SalesMarket, 
           ProductNumber) %>%
  summarise(Demand = sum(Demand)) %>%
  ungroup() %>%
  sdf_register("voy_demand")

tbl_cache(sc, "voy_demand")

# COMMAND ----------

voy_demand

# COMMAND ----------

voy_demand %>% 
  group_by(Date) %>%
  summarise(y = sum(Demand)) %>%
  arrange(Date)

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
            group_by(Date, ProductNumber) %>%
            summarise(TotalOrders = sum(Quantity)) %>%
            ungroup()

# COMMAND ----------

test <- voy_demand %>%
  left_join(income_pn_dc_sm)

# COMMAND ----------



# COMMAND ----------

income_pn_dc_sm %>%
  group_by(Date) %>%
  summarise(
            TotalOrders = sum(TotalOrders, na.rm = TRUE)) %>%
  arrange(Date) %>%
  print(n = 100)

# COMMAND ----------

test %>%
  group_by(Date) %>%
  summarise(Demand = sum(Demand),
            TotalOrders = sum(TotalOrders, na.rm = TRUE)) %>%
  mutate(Diff = (Demand-TotalOrders)) %>%
  arrange(Date) %>%
  print(n = 100)


