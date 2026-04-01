# Databricks notebook source
# Reads dc order income quantity on ProductLine level
# Reads and adds scaled (with weighted working days) TotalOrders_scaled column on ProductLine level.
# Calculates weighted working days on ProductLine level for end_date + 60 months

# COMMAND ----------

# MAGIC %run /Repos/alejandro.kuratomi_hernandez@secotools.com/prophet_R_secotools/secotools/pipelines/mount_datalake

# COMMAND ----------

path_in <- paste0("/mnt/blob/data/dc_orderincome/", dc_order_latest) 
income <- spark_read_csv(sc, 
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
            mutate(DateMonth = TransactionDate %>% substr(1, 6) %>% paste0("01"),
                   SalesMarket = if_else(SalesMarket == "CA", "US", SalesMarket)) #NOTE

# COMMAND ----------

user <- dbutils.secrets.get(scope = "azure_secrets", key = "sqluser")
password <- dbutils.secrets.get(scope = "azure_secrets", key = "sqlpassword")

url <- "jdbc:sqlserver://seco-azure-info-prod.database.windows.net:1433;database=seco-azure-info-prod"
product_info <- spark_read_jdbc(sc,
                        name = "product_info",
                        options = list(user = user,
                        password = password,
                        url = url,
                        dbtable = "pub.view_ProductBasic")) %>%
  rename(ProductNumber = ItemNumber) %>%
  select(ProductNumber, ProductLine)

# COMMAND ----------

#path_in <- paste0("/mnt/blob/data/iw_dimvproduct/", iw_dimvproduct_latest) 
#product_info2 <- spark_read_csv(sc, 
#                       path = path_in,
#                       delimiter = ";"
#                                   ) %>%
#                  select(ProductNumber, ProductLine)

# COMMAND ----------

path_in <- paste0("/mnt/blob/utils/", "CompanyKeys.csv") 
CompanyKeys <- spark_read_csv(sc, 
                       path = path_in,
                       delimiter = ";"
                                   ) %>%
  filter(VisibleInSales == 1) %>%
  select(CompanyKey, CompanySecoName, CompanyId) %>%
  collect() 

# COMMAND ----------

path_in <- paste0("/mnt/blob/utils/", "CompanyWorkingDays.csv") 
working_days <- spark_read_csv(sc, 
                       path = path_in,
                       delimiter = ";"
                                   ) %>%
  select(CompanyKey, PeriodKey, Value) %>%
  collect()

working_days <- working_days %>%
  left_join(CompanyKeys) %>%
  rename(SalesMarket = CompanyId,
         DateMonth = PeriodKey,
         working_days = Value) %>%
  mutate(DateMonth = DateMonth %>% paste0("01") %>% as.Date(format = "%Y%m%d"),
         working_days = working_days %>% as.numeric()) %>%
  select(DateMonth, SalesMarket, working_days) %>%
  filter(DateMonth >= "2015-01-01", DateMonth <= "2022-12-01")

# COMMAND ----------

working_days <- working_days %>%
  mutate(month = month(DateMonth)) %>%
  group_by(month) %>%
  mutate(month_avg_total = mean(working_days, na.rm = TRUE)) %>%
  ungroup()

# COMMAND ----------

impute_working_days <- function(tbl) {
  
  sm <- tbl %>% pull(SalesMarket) %>% unique()
  tbl <- tbl %>%
    group_by(month) %>%
    mutate(month_avg_SalesMarket = mean(working_days, na.rm = TRUE)) %>%
    ungroup()
  
  tbl <- tbl %>%
    tidyr::complete(DateMonth = seq.Date(from = "2015-01-01" %>% as.Date,
                                  to = end_date %m+% months(60), by = "month")) %>%
    mutate(SalesMarket = sm,
           month = month(DateMonth)) %>%
    group_by(month) %>%
    mutate(month_avg_total = mean(month_avg_total, na.rm = TRUE),
           month_avg_SalesMarket = mean(month_avg_SalesMarket, na.rm = TRUE)) %>%
    ungroup() %>%
    mutate(working_days = if_else(is.na(working_days), month_avg_SalesMarket, working_days)) %>%
    mutate(working_days = if_else(is.na(working_days), month_avg_total, working_days))
  
  tbl %>%
    arrange(DateMonth)
}

# COMMAND ----------

working_days <- working_days %>%
  group_by(SalesMarket) %>%
  do(impute_working_days(.)) %>%
  ungroup()

# COMMAND ----------

income <- income %>%
  left_join(product_info) %>%
  group_by(ProductLine, SalesMarket, DateMonth) %>%
  summarise(TotalOrders = sum(Quantity)) %>%
  ungroup() %>%
  collect() %>%
  mutate(DateMonth = DateMonth %>% as.Date(format = "%Y%m%d"),
         ProductLine = if_else(is.na(ProductLine), "MISSING", ProductLine),
         SalesMarket = if_else(is.na(SalesMarket), "MISSING", SalesMarket)) %>%
  filter(DateMonth <= end_date)

# COMMAND ----------

sc %>% spark_session() %>% invoke("catalog") %>% 
  invoke("clearCache")

# COMMAND ----------

impute_dates <- function(tbl){
  pl <- tbl %>% pull(ProductLine) %>% unique()
  sm <- tbl %>% pull(SalesMarket) %>% unique()
  tbl %>% 
    tidyr::complete(DateMonth = seq.Date(min(DateMonth), max(DateMonth), by = "month")) %>% 
    mutate(TotalOrders = replace(TotalOrders, is.na(TotalOrders), 0),
           ProductLine = pl,
           SalesMarket = sm) 
}

# COMMAND ----------

income <- income %>%
  group_by(ProductLine, SalesMarket) %>%
  do(impute_dates(.)) %>%
  ungroup()

# COMMAND ----------

smooth_outliers_US <- function(tbl) {
  library(forecast) 
  sm <- tbl %>% pull(SalesMarket) %>% unique()
  time_series <- ts(tbl %>% pull(TotalOrders), frequency = 12)
  TotalOrders_smoothed <- time_series %>% as.numeric()
  
  if (sm == "US" & (length(time_series) > 24)) TotalOrders_smoothed <- time_series %>% tsclean() %>% as.numeric()
  
  tbl %>% cbind(TotalOrders_smoothed)
}

# COMMAND ----------

income <- income %>%
  group_by(SalesMarket, ProductLine) %>%
  do(smooth_outliers_US(.)) %>%
  ungroup() %>%
  select(-TotalOrders) %>%
  rename(TotalOrders = TotalOrders_smoothed)

# COMMAND ----------

income <- income %>%
  left_join(working_days %>%
             select(DateMonth, 
                    SalesMarket,
                    working_days)) %>%
  group_by(month = month(DateMonth)) %>%
  mutate(avg_wd_total = mean(working_days, na.rm = TRUE)) %>%
  mutate(working_days = if_else(working_days %>% is.na(), avg_wd_total, working_days)) %>%
  ungroup()

# COMMAND ----------

income_raw <- income

# COMMAND ----------

income <- income %>%
  group_by(DateMonth, ProductLine) %>%
  mutate(p = abs(TotalOrders) / sum(abs(TotalOrders))) %>%
  mutate(p = if_else( is.na(p)|is.infinite(p), 1/length(p) , p )) %>%
  summarise(TotalOrders = sum(TotalOrders),
            weighted_wd = sum(p * working_days)) %>%
  ungroup()

# COMMAND ----------

smooth_outliers <- function(tbl) {
  library(forecast) 
  
  time_series <- ts(tbl %>% pull(TotalOrders), frequency = 12)
  TotalOrders_smoothed <- time_series %>% tsclean() %>% as.numeric()
  
  tbl %>% cbind(TotalOrders_smoothed)
}

# COMMAND ----------

income <- income %>%
  group_by(ProductLine) %>%
  do(smooth_outliers(.)) %>%
  ungroup() %>%
  rename(TotalOrders_raw = TotalOrders,
         TotalOrders = TotalOrders_smoothed) %>%
  mutate(TotalOrders = if_else(TotalOrders <= 0, 1, TotalOrders)) %>%
  mutate(TotalOrders_scaled = TotalOrders / weighted_wd)

# COMMAND ----------

income %>%
  group_by(DateMonth, ProductLine) %>%
  summarise(raw = sum(TotalOrders_raw),
            smooth = sum(TotalOrders)) %>%
  ggplot()+
  geom_line(aes(x = DateMonth, y = raw, color = ProductLine))+
  geom_line(aes(x = DateMonth, y = smooth, color = ProductLine), linetype = "dashed")

# COMMAND ----------

add_guesstimate <- function(tbl, ...) {
  library(lubridate)
  tbl <- tbl %>%
    mutate(month = month(DateMonth))
  
  L <- tbl %>% nrow()
  scaled_means <- c(NaN)
  for(i in 2:L) {
    m <- tbl %>% slice(i) %>% pull(month)
    sc_m <- tbl %>% slice(1:(i-1)) %>% filter(month == m) %>% pull(TotalOrders_scaled)
    scaled_means <- c(scaled_means, sc_m %>% mean(na.rm = TRUE))
  }
  tbl %>%
    mutate(guesstimate = scaled_means) %>%
    select(-month)
}

# COMMAND ----------

income <- income %>%
  group_by(ProductLine) %>%
  do(add_guesstimate(.)) %>%
  ungroup() %>%
  mutate(guesstimate = guesstimate * weighted_wd)

# COMMAND ----------

pl_sm_proportions <- income_raw %>%
  filter(DateMonth >= end_date - 365) %>%
  group_by(ProductLine, SalesMarket) %>%
  summarise(sm_pl = sum(TotalOrders)) %>%
  mutate(sm_pl_prop = sm_pl / sum(sm_pl)) %>%
  ungroup()

# COMMAND ----------

pl_wd_future <- pl_sm_proportions %>%
  left_join(working_days %>%
             select(DateMonth, SalesMarket, working_days)) %>%
  group_by(DateMonth, ProductLine) %>%
  summarise(weighted_wd = sum(sm_pl_prop * working_days, na.rm = TRUE) / sum(sm_pl_prop)) %>%
  ungroup() %>%
  filter(!is.na(DateMonth))
    


