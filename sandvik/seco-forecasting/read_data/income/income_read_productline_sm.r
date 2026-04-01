# Databricks notebook source
# Reads dc order income quantity on ProductLine level
# Reads and adds scaled (with weighted working days) TotalOrders_scaled column on ProductLine level.
# Calculates weighted working days on ProductLine level for end_date + 60 months

# COMMAND ----------

# MAGIC %run /Repos/alejandro.kuratomi_hernandez@secotools.com/prophet_R_secotools/sandvik/seco-forecasting/read_data/mount_datalake

# COMMAND ----------

user <- dbutils.secrets.get(scope = "azure_secrets", key = "sqluser")
password <- dbutils.secrets.get(scope = "azure_secrets", key = "sqlpassword")

url <- "jdbc:sqlserver://seco-azure-info-prod.database.windows.net:1433;database=seco-azure-info-prod"
wds_raw <- spark_read_jdbc(sc,
                        name = "wds",
                        options = list(user = user,
                        password = password,
                        url = url,
                        dbtable = "pub.view_WorkingDays")) %>%
  group_by(SalesMarket, WorkingDaysDate) %>%
  summarise(working_days = mean(WorkingDays)) %>%
  rename(DateMonth = WorkingDaysDate) %>%
  collect() %>%
  filter(complete.cases(.))

wds <- wds_raw %>%
  filter(complete.cases(.))

# COMMAND ----------

path_in <- paste0("/mnt/blob/data/dc_orderincome/", dc_order_latest) 
income <- spark_read_csv(sc, 
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
            mutate(DateMonth = regexp_replace(TransactionDate, "-", "") %>% substr(1, 6) %>% paste0("01"),
                   SalesMarket = if_else(SalesMarket == "CA", "US", SalesMarket)) %>%
            mutate(SalesMarket = if_else(is.na(SalesMarket), "MISSING", SalesMarket)) %>%
       mutate(SalesMarket = if_else(SalesMarket == "IP", "IN", SalesMarket)) %>%
       filter(ProductNumber != "03080586")

# COMMAND ----------

income %>% glimpse()

# COMMAND ----------

income %>%
  group_by(DateMonth) %>%
  summarise(TotalOrders = sum(Quantity)) %>%
  arrange(DateMonth) %>%
  print(n = 300)

# COMMAND ----------

path_in <- "/mnt/blob/utils/SalesRegions.csv"
SalesRegions <- spark_read_csv(sc, 
                       path = path_in,
                       delimiter = ";"
                                   ) %>%
  collect() %>%
  rename(SalesMarket = Company_id) %>%
  select(SalesMarket, Sales_Region)

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

wds <- wds %>%
  mutate(month = month(DateMonth)) %>%
  group_by(month) %>%
  mutate(month_avg_total = mean(working_days, na.rm = TRUE)) %>%
  ungroup()

# COMMAND ----------

wds %>% filter(DateMonth >= "2024-01-01") %>% arrange(DateMonth) %>%
print(n = 500) 

# COMMAND ----------

impute_working_days <- function(tbl) {
  
  sm <- tbl %>% pull(SalesMarket) %>% unique()
  tbl <- tbl %>%
    ungroup()
  
  new_wds <- tbl %>%
    filter(DateMonth >= "2021-01-01") %>% #Update in working days BPC
    group_by(month) %>%
    summarise(month_avg_SalesMarket = mean(working_days, na.rm = TRUE)) 
  
  tbl <- tbl %>%
    tidyr::complete(DateMonth = seq.Date(from = "2015-01-01" %>% as.Date,
                                  to = end_date %m+% months(60), by = "month")) %>%
    mutate(SalesMarket = sm,
           month = month(DateMonth)) %>%
    left_join(new_wds, by = "month") %>%
    mutate(working_days = if_else(is.na(working_days), month_avg_SalesMarket, working_days)) %>%
    mutate(working_days = if_else(is.na(working_days), month_avg_total, working_days))
  
  tbl <- tbl %>%
    left_join(SalesRegions, by = "SalesMarket") %>%
    mutate(working_days = ifelse(Sales_Region == "Europe" & (month %in% c(7,8)) & (DateMonth <= "2021-01-01"), NA, working_days)) %>% ##Vaccation in Europe is in > 2021 working days data but not before
    mutate(working_days = if_else(is.na(working_days), month_avg_SalesMarket, working_days)) %>%
    mutate(working_days = if_else(is.na(working_days), month_avg_total, working_days)) %>%
    mutate(working_days = if_else(working_days < 5, month_avg_SalesMarket, working_days)) %>%
    mutate(working_days = if_else(working_days < 5, month_avg_total, working_days))
  
  tbl %>%
    arrange(DateMonth)
  
}

# COMMAND ----------

wds <- wds %>%
  group_by(SalesMarket) %>%
  do(impute_working_days(.)) %>%
  ungroup()

# COMMAND ----------

wds %>% 
group_by(DateMonth) %>%
summarise(working_days = mean(working_days),
          month_avg_total = mean(month_avg_total, na.rm = TRUE)) %>%
          print(n = 200)


# COMMAND ----------

wds_spark <- sdf_copy_to(sc, wds, overwrite=TRUE)
path_out <- paste0("/mnt/blob/utils/wds.csv")
spark_write_csv(wds_spark, 
                path = path_out, 
                delimiter = ";", 
                mode = "overwrite")

# COMMAND ----------

income <- income %>%
  left_join(product_info) %>%
  mutate(ProductLine = if_else(is.na(ProductLine), "MISSING", ProductLine),
         SalesMarket = if_else(is.na(SalesMarket), "MISSING", SalesMarket)) %>%
  group_by(ProductLine, SalesMarket, DateMonth) %>%
  summarise(TotalOrders = sum(Quantity)) %>%
  ungroup() %>%
  collect() %>%
  mutate(DateMonth = DateMonth %>% as.Date(format = "%Y%m%d")) %>%
  filter(DateMonth <= end_date,
         DateMonth >= "2019-01-01")

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
  list.of.packages <- c("forecast")
  new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
  
  #if(length(new.packages)){
  #  install.packages(new.packages) 
  #}  
    library(forecast)
    sm <- tbl %>% pull(SalesMarket) %>% unique()
  
    time_series <- ts(tbl %>% pull(TotalOrders), frequency = 12)
    TotalOrders_smoothed <- time_series %>% as.numeric()
  
    if (sm == "US" & (length(time_series) > 24)) TotalOrders_smoothed <- time_series %>% tsclean() %>% as.numeric()
    if (sm == "MA" & (length(time_series) > 24)) TotalOrders_smoothed <- time_series %>% tsclean() %>% as.numeric()
    if ((length(time_series) > 24)) {
    
      tmp_smoothed <- time_series %>% tsclean() %>% as.numeric()
      index <- tbl %>%
        mutate(index = 1:n()) %>%
        filter(DateMonth %in% c("2022-08-01" %>% as.Date())) %>% #Outliers with a lot of pre-buys in aug 2022
        filter(DateMonth %in% c("2025-10-01" %>% as.Date())) %>% #Outliers with a lot of pre-buys in oct 2025
      pull(index)
      
      if (length(index) > 0) TotalOrders_smoothed[index] <- tmp_smoothed[index]

    }
    
  
  tbl %>% cbind(TotalOrders_smoothed)
}

# COMMAND ----------

income <- income %>%
  group_by(SalesMarket, ProductLine) %>%
  do(smooth_outliers_US(.)) %>%
  ungroup() %>%
  mutate(TotalOrders_raw = TotalOrders) %>%
  select(-TotalOrders) %>%
  rename(TotalOrders = TotalOrders_smoothed)  

# COMMAND ----------

income <- income %>%
  left_join(wds %>%
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
  group_by(DateMonth, ProductLine, SalesMarket) %>%
  summarise(TotalOrders = sum(TotalOrders),
            TotalOrders_raw = sum(TotalOrders_raw),
            weighted_wd = working_days) %>%
  ungroup()

# COMMAND ----------

income <- income %>%
  mutate(TotalOrders = if_else(TotalOrders <= 0, 0, TotalOrders)) %>%
  mutate(TotalOrders_scaled = TotalOrders / weighted_wd) 

# COMMAND ----------

wds %>%
  arrange(working_days)

# COMMAND ----------

income %>%
  group_by(DateMonth) %>%
  summarise(y = sum(TotalOrders_raw),
            ysmooth = sum(TotalOrders),
            y_scaled = sum(TotalOrders_scaled)) %>%
            print(n = 200)

# COMMAND ----------

income %>%
  group_by(DateMonth) %>%
  summarise(y = sum(TotalOrders_raw),
            ysmooth = sum(TotalOrders),
            y_scaled = sum(TotalOrders_scaled)) %>%
  ggplot()+
  geom_line(aes(x = DateMonth, y = y))+
  geom_line(aes(x = DateMonth, y = ysmooth), linetype = "dashed")+
  geom_line(aes(x = DateMonth, y = y_scaled), linetype = "twodash")

# COMMAND ----------

income %>%
  group_by(DateMonth) %>%
  summarise(y = sum(TotalOrders_raw),
            ysmooth = sum(TotalOrders)) %>%
  mutate(diff = y - ysmooth) %>%
  print(n = 400)

# COMMAND ----------

income %>%
  group_by(DateMonth) %>%
  summarise(y = sum(TotalOrders_raw),
            ysmooth = sum(TotalOrders)) %>%
  mutate(diff = y - ysmooth) %>%
  print(n = 400)

# COMMAND ----------

wds %>%
  filter(SalesMarket == 'SE') %>%
  arrange(DateMonth) %>%
  print(n = 200)

# COMMAND ----------

wds_raw %>%
  filter(SalesMarket == 'SE') %>%
  arrange(DateMonth) %>%
  print(n = 200)
