# Databricks notebook source
# MAGIC %run /Repos/alejandro.kuratomi_hernandez@secotools.com/prophet_R_secotools/secotools/pipelines/invoice_read_pn_dc_sm

# COMMAND ----------

invoice_pn_dc_sm <- invoice_pn_dc_sm %>%
  group_by(SupplyingWarehouse, SalesMarket, ProductNumber) %>%
  mutate(latest_order = max(DateMonth),
         nr_orders = n(),
         part = sample(1:(20*8), 1)) %>%
  ungroup() %>%
  filter(latest_order >= (end_date-365)) %>%
  select(-c(latest_order, nr_orders))

# COMMAND ----------

library(M4metalearning)
library(forecast)
create_list <- function(data){
  library(tidyverse)
  to_list <- function(tbl){
    library(M4metalearning)
    library(forecast)
    
  impute_dates <- function(tbl){
    pn <- tbl %>% pull(ProductNumber) %>% unique()
    sw <- tbl %>% pull(SupplyingWarehouse) %>% unique()
    sm <- tbl %>% pull(SalesMarket) %>% unique()
    tbl %>% 
      tidyr::complete(DateMonth = seq.Date(min(DateMonth), end_date, by = "month")) %>% 
      mutate(TotalOrders = replace(TotalOrders, is.na(TotalOrders), 0),
             ProductNumber = pn,
             SupplyingWarehouse = sw,
             SalesMarket = sm) 
  }
    
    pn <- tbl %>% pull(ProductNumber) %>% unique()
    sw <- tbl %>% pull(SupplyingWarehouse) %>% unique()
    sm <- tbl %>% pull(SalesMarket) %>% unique()
    
    tbl <- tbl %>% impute_dates()
    
    h <- 60
    dates <- tbl %>% pull(DateMonth)
    values <- ts(tbl %>% pull(TotalOrders), frequency = 12)
    if( length(unique(values)) == 1 ) values <- ts(values + rnorm(length(values), sd = 0.1), frequency = 12)
      
    list(
      ProductNumber = pn,
      SupplyingWarehouse = sw,
      SalesMarket = sm,
      x = values,
      h = h,
      Date = dates,
      L = length(values)
    )
  }
  data %>%
    group_split(ProductNumber, SupplyingWarehouse, SalesMarket) %>%
    lapply(., to_list)
}

sma_list <- SparkR::spark.lapply(invoice_pn_dc_sm %>% group_split(part), func = create_list) %>% unlist(recursive = FALSE)

# COMMAND ----------

create_list_for_spark <- function(lst_data){
  
  nr_workers <- spark_context(sc) %>% invoke("getExecutorMemoryStatus") %>% names() %>% length() - 1
  nr_cores <- 8
  total_cores <-  nr_cores * nr_workers
  lst_for_spark <- list()
  subsequences <- split(1:length(lst_data), 1:total_cores)
  for (i in 1:total_cores) {
    lst_for_spark[[i]] <- lst_data[subsequences[[i]]]
  }
  lst_for_spark
}

# COMMAND ----------

to_tbl <- function(lst, end_date = end_date){
  library(tidyverse)
  library(lubridate)
  pn <- lst$ProductNumber
  sw <- lst$SupplyingWarehouse
  sm <- lst$SalesMarket
  h <- lst$h
  
  not_zero_i <- which(lst$x != 0)
  x <- lst$x[not_zero_i]
  dates <- lst$Date %>% as.Date()
  old_dates <- dates[not_zero_i]
  values <- c(x, lst$y_hat)
  
  
  source <- c(rep("x", x %>% length()),
              rep("yhat", h))
  
  new_dates <- seq.Date(end_date, 
                       length.out = h+1, 
                       by = "month")[-1]
  
  dates <- c(old_dates, new_dates)
  tryCatch(tibble(Date = dates,
         ProductNumber = pn,
         SupplyingWarehouse = sw,
         SalesMarket = sm,
         values = values,
         source = source),
         error = function(e) print(lst))
}

# COMMAND ----------

caluclate_forecast_sma <- function(lst){
  library(M4metalearning)
  library(tidyverse)
  library(smooth)
  sma_forecast <<- function(x, h){
    m <- sma(y = x %>% as.numeric(), 
             h = 60,
             ic = "AIC", 
             silent = "all")
    pred <- m$forecast %>% as.numeric()
    pred
  }
   calc_forecasts(lst, list("sma_forecast"), n.cores = 1)
}

# COMMAND ----------

sma_list <- SparkR::spark.lapply(sma_list %>% create_list_for_spark(), func = caluclate_forecast_sma) %>% unlist(recursive = FALSE)
sma_list <- ensemble_forecast(matrix(1, nrow = length(sma_list)), sma_list)

# COMMAND ----------

library(parallel)
library(data.table)
cl <- makeCluster(15)
forecast_tbl <- rbindlist(parLapply(cl, sma_list, to_tbl, end_date = end_date)) %>% as_tibble()
stopCluster(cl)

# COMMAND ----------

forecast_tbl %>%
  group_by(Date, source, SupplyingWarehouse) %>%
  summarise(y = sum(values)) %>%
  ggplot()+
  geom_line(aes(x = Date, y = y, color = SupplyingWarehouse))

# COMMAND ----------

forecast_tbl %>%
  group_by(Date, source) %>%
  summarise(y = sum(values)) %>%
  ggplot()+
  geom_line(aes(x = Date, y = y, color = source))

# COMMAND ----------

date_today <- Sys.Date() %>% str_replace_all(pattern = "-", replacement = "_")

# COMMAND ----------

path_out_current <- paste0("/mnt/blob/forecast/invoice/productnumber_dc_sm/", "current", "/pn_dc_sm.csv")
path_out_archive <- paste0("/mnt/blob/forecast/invoice/productnumber_dc_sm/archive/", date_today, "/pn_dc_sm.csv")

dbutils.fs.rm(path_out_current, TRUE)
dbutils.fs.rm(path_out_archive, TRUE)


forecast_tbl <- forecast_tbl %>%
  filter(source != "x")

delta <- floor(nrow(forecast_tbl) / 100)
for (i in 1:100) {
  
  j <- i * delta
  if( i == 100 ) j <- nrow(forecast_tbl)
  
  print(c(i, (i-1) * delta+1, j))
  tmp_tbl <- forecast_tbl %>%
               dplyr::slice( ((i-1) * delta+1) : j)
  
  tmp_spark <- sdf_copy_to(sc, 
                           tmp_tbl,
                           overwrite = TRUE)
  
  spark_write_csv(tmp_spark, 
                path = path_out_current, 
                delimiter = ";", 
                mode = "append")
  spark_write_csv(tmp_spark, 
                path = path_out_archive, 
                delimiter = ";", 
                mode = "append")
  
  sc %>% spark_session() %>% invoke("catalog") %>% 
    invoke("clearCache")
}


