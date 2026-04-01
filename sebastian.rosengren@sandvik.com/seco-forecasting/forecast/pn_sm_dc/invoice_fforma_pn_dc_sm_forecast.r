# Databricks notebook source
# MAGIC %run /Repos/sebastian.rosengren@sandvik.com/seco-forecasting/read_data/invoice/invoice_read_pn_dc_sm

# COMMAND ----------

income_pn_dc_sm <- income_pn_dc_sm %>%
  filter(complete.cases(.)) %>%
  mutate(TotalOrders = TotalOrders_no_outliers) %>%
  select(DateMonth, SupplyingWarehouse, SalesMarket, ProductNumber, TotalOrders)

# COMMAND ----------

income_pn_dc_sm <- income_pn_dc_sm %>%
  group_by(SupplyingWarehouse, SalesMarket, ProductNumber) %>%
  mutate(latest_order = max(DateMonth),
         nr_orders = n(),
         part = sample(1:(20*8), 1)) %>%
  ungroup() %>%
  filter(latest_order >= (end_date-365)) %>%
  select(-c(latest_order, nr_orders))

# COMMAND ----------

income_pn_dc_sm %>%
  group_by(DateMonth) %>%
  summarise(y = sum(TotalOrders)) %>%
  print(n = 200)

# COMMAND ----------

income_pn_dc_sm %>%
  group_by(DateMonth) %>%
  summarise(y = sum(TotalOrders)) %>%
  ggplot()+
  geom_line(aes(x = DateMonth, y = y))

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

temporal_list <- SparkR::spark.lapply(income_pn_dc_sm %>% group_split(part), func = create_list) %>% unlist(recursive = FALSE)

# COMMAND ----------

library(M4metalearning)

# COMMAND ----------

L_index <- c()
S_val <- c()

for(i in 1:length(temporal_list)){
  L <- temporal_list[[i]]$L
  S <- temporal_list[[i]]$x %>% sum()
  
  if (L > 24) {
    L_index <- c(L_index, i)
    S_val <- c(S_val, S)
  }
  
}

index_tbl <- tibble(L = L_index, S = S_val) %>%
  arrange(-S_val) %>%
  dplyr::slice(1:10000)

L_index <- index_tbl %>% pull(L)
fforma_list <- temporal_list[L_index]
sma_list <- temporal_list[-L_index]

# COMMAND ----------

calculate_forecast <- function(lst){
  library(forecast)
  library(M4metalearning)
  library(tidyverse)
  library(smooth)
  
  
  unlockBinding("naive_forec", as.environment("package:M4metalearning"))
  assignInNamespace("naive_forec", 
                    function(x, h) { 
                      model <- forecast::naive(x, h)
                      forecast::forecast(model, h = h)$mean },
                    ns = "M4metalearning", 
                    envir = as.environment("package:M4metalearning"))
  assign("naive_forec", 
         function(x, h) { 
           model <- forecast::naive(x, h)
           forecast::forecast(model, h = h)$mean }, 
         envir = as.environment("package:M4metalearning"))
  
  unlockBinding("rw_drift_forec", as.environment("package:M4metalearning"))
  assignInNamespace("rw_drift_forec", 
                    function(x, h) { 
                      model <- forecast::rwf(x, h)
                      forecast::forecast(model, h = h)$mean },
                    ns = "M4metalearning", 
                    envir = as.environment("package:M4metalearning"))
  assign("rw_drift_forec", 
         function(x, h) { 
           model <- forecast::rwf(x, h)
           forecast::forecast(model, h = h)$mean }, 
         envir = as.environment("package:M4metalearning"))
   
  sma_forecast <<- function(x, h){
    m <- sma(y = x, 
             h = h,
             ic = "AICc")
    pred <- m$forecast
    pred
  }
  
  arima_aug <<- function(x, h, dates) {
    historic_aug <<- (dates %in% seq.Date("2015-08-01" %>% as.Date(), "2022-10-01" %>% as.Date(), by = "year")) %>% as.numeric()
    historic_covid <<- (dates %in% seq.Date("2020-04-01" %>% as.Date(), "2020-06-01" %>% as.Date(), by = "month")) %>% as.numeric()
    
    
    
    future_dates <- future_dates <- seq.Date(max(dates), length.out = h+1 , by = "month")[-1]
    
    model <- forecast:::auto.arima( x, stepwise = FALSE,
                                    xreg = cbind(historic_aug, historic_covid) )
    
    future_aug <<- (future_dates %in% seq.Date("2015-08-01" %>% as.Date(), "2022-10-01" %>% as.Date(), by = "year")) %>% as.numeric()
    future_covid <<- (future_dates %in% seq.Date("2020-04-01" %>% as.Date(), "2020-06-01" %>% as.Date(), by = "month")) %>% as.numeric()
    
    xregmat <<- cbind(future_aug, future_covid)
    
    
    colnames(xregmat) <- c("historic_aug", "historic_covid") 
    
    forecast::forecast( model, h = h,
                        xreg = xregmat )$mean
     
  }
  
  stl_arima <<- function(x, h, dates) {
    historic_aug <<- (dates %in% seq.Date("2015-08-01" %>% as.Date(), "2022-10-01" %>% as.Date(), by = "year")) %>% as.numeric()
    historic_covid <<- (dates %in% seq.Date("2020-04-01" %>% as.Date(), "2020-06-01" %>% as.Date(), by = "month")) %>% as.numeric()
  
    future_dates <- future_dates <- seq.Date(max(dates), length.out = h+1 , by = "month")[-1]
    
    model <- stlm( x, method = "arima",
                                    xreg = cbind(historic_aug, historic_covid), stepwise = FALSE) #added stepwise = FALSE 
    
    future_aug <<- (future_dates %in% seq.Date("2015-08-01" %>% as.Date(), "2022-10-01" %>% as.Date(), by = "year")) %>% as.numeric()
    future_covid <<- (future_dates %in% seq.Date("2020-04-01" %>% as.Date(), "2020-06-01" %>% as.Date(), by = "month")) %>% as.numeric()
    
    xregmat <<- cbind(future_aug, future_covid)
    
    
    colnames(xregmat) <- c("historic_aug", "historic_covid")
    
    forecast::forecast( model, h = h,
                        xreg = xregmat )$mean
     
  }  

  stlf_forecast <<- function(x, h) {
    
    stlf(x, h)$mean
     
  }
 
  

  calc_forecast_seb <- function (dataset, methods, n.cores = 1) {

    process_forecast_methods <- function(seriesdata, methods_list) {

      #process each method in methods_list to produce the forecasts and the errors
      lapply(methods_list, function (mentry) {
        method_name <- mentry
        print(method_name)
        
        method_fun <- get(mentry)
        if (method_name %in% c("arima_aug", "sarima_aug", "stl_arima")) {
              forecasts <- tryCatch( method_fun(x = seriesdata$x, 
                                                h = seriesdata$h, 
                                                dates = seriesdata$Date),
                               error=function(error) {
                                 print(error)
                                 print(paste("ERROR processing series: ", seriesdata$ProductLine))
                                 print(paste("The forecast method that produced the error is:",
                                             method_name))
                                 print("Returning snaive forecasts instead")
                                 snaive_forec(seriesdata$x, seriesdata$h)
                               })
        }
        else if (method_name %in% c("prophet_forecast")) {
              forecasts <- tryCatch( method_fun(x = seriesdata$x,
                                                dates = seriesdata$Date,
                                                h = seriesdata$h,
                                                hp = seriesdata$hp,
                                                g = seriesdata$guesstimate),
                               error=function(error) {
                                 print(error)
                                 print(paste("ERROR processing series: ", seriesdata$ProductLine))
                                 print(paste("The forecast method that produced the error is:",
                                             method_name))
                                 print("Returning snaive forecasts instead")
                                 snaive_forec(seriesdata$x, seriesdata$h)
                               })          
        }
        else {
        forecasts <- tryCatch( method_fun(x=seriesdata$x, h=seriesdata$h),
                               error=function(error) {
                                 print(error)
                                 print(paste("ERROR processing series: ", seriesdata$ProductLine))
                                 print(paste("The forecast method that produced the error is:",
                                             method_name))
                                 print("Returning snaive forecasts instead")
                                 snaive_forec(seriesdata$x, seriesdata$h)
                               } )
          }
        print(forecasts)
        list( forecasts=forecasts, method_name=method_name )
      })
    }
  list_process_fun <- lapply
  
  ret_list <- list_process_fun(dataset, function(seriesdata) {
    #print(seriesdata)
    results <- process_forecast_methods(seriesdata, methods)
    #print("--------")
    ff <- t(sapply(results, function(resentry) resentry$forecasts))
    #dim(ff) <- c(length(methods), seriesdata$h)              
    method_names <- sapply(results, function(resentry) resentry$method_name)
    #print(method_names)                       
    row.names(ff) <- method_names
    seriesdata$ff <- ff 
    seriesdata
                           
                           })
 
  ret_list
}              
                   
   forecast_strings <- forec_methods()
   forecast_strings[forecast_strings == "nnetar_forec"] <- NULL
   forecast_strings[forecast_strings == "tbats_forec"] <- NULL    
                   
   calc_forecast_seb(lst, append(forecast_strings, c("sma_forecast",                
                                   "arima_aug",
                                   "stlf_forecast",
                                   "stl_arima"
                                   )), n.cores = 1)
                   
                
}

# COMMAND ----------

create_list_for_spark <- function(lst_data){
  
  nr_workers <- spark_context(sc) %>% invoke("getExecutorMemoryStatus") %>% names() %>% length() - 1
  nr_cores <- 8
  total_cores <- 2 * nr_cores * nr_workers
  lst_for_spark <- list()
  subsequences <- split(1:length(lst_data), 1:total_cores)
  for (i in 1:total_cores) {
    lst_for_spark[[i]] <- lst_data[subsequences[[i]]]
  }
  lst_for_spark
}

# COMMAND ----------

fforma_list <- SparkR::spark.lapply(fforma_list %>% create_list_for_spark(), func = calculate_forecast) %>% unlist(recursive = FALSE)

# COMMAND ----------

fforma_list_raw <- fforma_list

# COMMAND ----------

fforma_list <- fforma_list %>% THA_features(n.cores = 15)

# COMMAND ----------

null_index <- c()
for(i in 1:length(fforma_list)) {
  feat <- fforma_list[[i]]$features
  ff <- fforma_list[[i]]$ff
  if(is.null(feat)) null_index <- c(null_index, i)
  if((ff %>% as.vector() %>% is.na() %>% sum() > 0)) null_index <- c(null_index, i)
}
 
sma_list <- append(sma_list, fforma_list_raw[null_index %>% unique()])
fforma_list[null_index %>% unique()] <- NULL

for(i in 1:length(fforma_list)) {  
  colnames(fforma_list[[i]]$features) <- colnames(fforma_list[[i]]$features) %>% str_replace(pattern="\\.", "_")
}

# COMMAND ----------

rm("fforma_list_raw")

# COMMAND ----------

for(i in 1:length(fforma_list)){
  ff <- fforma_list[[i]]$ff
  ff[ ff < 0 ] <- 0
  fforma_list[[i]]$ff <- ff
}

# COMMAND ----------

library(xgboost)

# COMMAND ----------

path_in <- paste0("/mnt/blob/hyperparameters/fforma/invoice/current/data_pn_dc_sm.csv")
xgb_data <- spark_read_csv(sc, 
                       path = path_in,
                       delimiter = ";"
                                    ) %>%
  arrange(index) %>%
  collect() %>%
  select(-index) %>%
  as.matrix()

path_in <- paste0("/mnt/blob/hyperparameters/fforma/invoice/current/errors_pn_dc_sm.csv")
xgb_errors <- spark_read_csv(sc, 
                       path = path_in,
                       delimiter = ";"
                                    ) %>%
  arrange(index) %>%
  collect() %>%
  select(-index) %>%
  as.matrix()

# COMMAND ----------

library(xgboost)

# COMMAND ----------

path_in <- paste0("/mnt/blob/hyperparameters/fforma/invoice/current/xgb_params_pn_dc_sm.csv")
param <- spark_read_csv(sc, 
                       path = path_in,
                       delimiter = ";"
                                    ) %>%
  collect() 

# COMMAND ----------

pred_data <- create_feat_classif_problem(fforma_list)

param <- list(max_depth = param %>% pull(max_depth),
              eta = param %>% pull(eta),
              nthread = 3,
              nrounds = param %>% pull(nrounds),
              silent = 1,
              objective = error_softmax_obj,
              num_class = ncol(xgb_errors), #the number of forecast methods used
              subsample = param %>% pull(subsample),
              colsample_bytree = param %>% pull(colsample_bytree))

meta_model <- train_selection_ensemble(xgb_data,
                                       xgb_errors,
                                       param = param)
preds <- predict_selection_ensemble(meta_model, pred_data$data %>% xgb.DMatrix())

# COMMAND ----------

fforma_list <- ensemble_forecast(preds, fforma_list)

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

#forecast_tbl <- forecast_list %>% lapply(to_tbl)
#library(data.table)
#forecast_tbl <- rbindlist(forecast_tbl) %>% as_tibble()

# COMMAND ----------

null_feats <- c()
for(i in 1:length(sma_list)) {
  if(sma_list[[i]]$x %>% is.null()) null_feats <- c(null_feats, i)
}
sma_list[null_feats] <- NULL

# COMMAND ----------

caluclate_forecast_sma <- function(lst){
  library(M4metalearning)
  library(tidyverse)
  library(smooth)
  sma_forecast <<- function(x, h){
    if (length(x) == 1) x <- c(x,x)
    m <- sma(y = x %>% as.numeric(), 
             h = 60,
             ic = "AIC")
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
forecast_tbl <- rbindlist(parLapply(cl, append(fforma_list, sma_list), to_tbl, end_date = end_date)) %>% as_tibble()
stopCluster(cl)

# COMMAND ----------

library(parallel)
library(data.table)
cl <- makeCluster(15)
forecast_tbl_fforma <- rbindlist(parLapply(cl, fforma_list, to_tbl, end_date = end_date)) %>% as_tibble()
stopCluster(cl)

# COMMAND ----------

forecast_tbl_fforma %>%
  group_by(Date, source) %>%
  summarise(y = sum(values)) %>%
  ggplot()+
  geom_line(aes(x = Date, y = y, color = source))

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