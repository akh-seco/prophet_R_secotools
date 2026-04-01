# Databricks notebook source
# MAGIC %run /Repos/sebastian.rosengren@sandvik.com/seco-forecasting/read_data/income/income_read_productline_sm

# COMMAND ----------

path_in <- paste0("/mnt/blob/hyperparameters/prophet/income/current/prophet_hp.csv")
hp_prophet <- spark_read_csv(sc, 
                       path = path_in,
                       delimiter = ";"
                                   ) %>%
  collect() %>%
  filter(scaled == 0, guess == 0, august == 1, sm == "additive") %>%
  group_by(ProductLine) %>%
  filter(mape == min(mape)) %>%
  arrange(mape)

# COMMAND ----------

income

# COMMAND ----------

income <- income %>%
  group_by(ProductLine, SalesMarket) %>%
  mutate(latest_order = max(DateMonth),
         tmp = sum(TotalOrders)) %>%
  ungroup() %>%
  filter(latest_order >= (end_date-365),
         tmp > 0) %>%
  select(-c(latest_order, tmp))

# COMMAND ----------

#add impute to end_date

library(M4metalearning)
library(forecast)
create_list <- function(data){
  to_list <- function(tbl){
    
    pl <- tbl %>% pull(ProductLine) %>% unique()
    sm <- tbl %>% pull(SalesMarket) %>% unique()
    
    impute_dates <- function(tbl){
      pl <- tbl %>% pull(ProductLine) %>% unique()
      sm <- tbl %>% pull(SalesMarket) %>% unique()
      tbl %>% 
        tidyr::complete(DateMonth = seq.Date(min(DateMonth), end_date, by = "month")) %>% 
        mutate(TotalOrders_scaled = replace(TotalOrders_scaled, is.na(TotalOrders_scaled), 0),
               ProductLine = pl,
               SalesMarket = sm) 
    }
    
    tbl <- tbl %>% impute_dates()
    
    
    h <- 60
    dates <- tbl %>% pull(DateMonth)
    values <- ts(tbl %>% pull(TotalOrders_scaled), frequency = 12)
    
    list(
      ProductLine = pl,
      SalesMarket = sm,
      x = values,
      h = h,
      L = values %>% length(),
      Date = dates,
      hp = hp_prophet %>% filter(ProductLine == pl)
    )
  }
  data %>%
    group_split(ProductLine, SalesMarket) %>%
    lapply(., to_list)
}

forecast_list <- create_list(income)

# COMMAND ----------

L_index <- c()
for(i in 1:length(forecast_list)){
  L <- forecast_list[[i]]$L
  if( L < 25) L_index <- c(L_index, i)
}
sma_list <- forecast_list[L_index]
forecast_list[L_index] <- NULL

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
 
  prophet_forecast <<- function(x, dates, h, hp){
    library(tidyverse)
    library(prophet)

    cp <- hp %>% pull(cps)
    sp <- hp %>% pull(sps)
    sm <- hp %>% pull(sm)
    cov <- hp %>% pull(covid)
    fo <- hp %>% pull(fo)
    aug <- hp %>% pull(august)
    

    future_dates <- seq.Date(max(dates), length.out = h+1 , by = "month")[-1]


    prophet_data <- tibble(
       ds = seq.Date(from = "2015-01-01" %>% as.Date(), length.out = (x %>% length()), by = "day"),
       y =  x %>% as.numeric(),
       checksum = dates
      ) %>%
        mutate(covid_ind = checksum %in% seq.Date("2020-04-01" %>% as.Date(), "2020-08-01" %>% as.Date(), by = "month"),
               aug_ind = checksum %in% setdiff(seq.Date("2015-08-01" %>% as.Date(), "2027-10-01" %>% as.Date(), by = "year"), "2022-08-01" %>% as.Date()))


    m <- prophet(
       daily.seasonality = FALSE,
       yearly.seasonality = FALSE,
       weekly.seasonality = FALSE,
       changepoint.prior.scale = cp,
       changepoint.range = 0.95
       )

     m <- add_seasonality(m, "yrly",
       period = 12,
       fourier.order = fo,
       mode = sm,
       prior.scale = sp
        )

    if ( cov == 1 ) m <- add_regressor(m, "covid_ind", standardize = FALSE)
    if ( aug == 1 ) m <- add_regressor(m, "aug_ind", standardize = FALSE)

    m <- fit.prophet(m,
           df = prophet_data
          )
    future_points <- make_future_dataframe(m, 
                                            periods = h, 
                                            freq = "day",
                                            include_history = FALSE)
     future_points <- future_points %>%
        mutate(covid_ind = (future_dates %in% seq.Date("2020-04-01" %>% as.Date(), "2020-08-01" %>% as.Date(), by = "month")) %>% as.numeric(),
               aug_ind = (future_dates %in% seq.Date("2015-08-01" %>% as.Date(), "2027-10-01" %>% as.Date(), by = "year")) %>% as.numeric())

    
     pred <- predict(m, future_points)
     pred$yhat
  }
  
  adam_forecast <<- function(x, h){
    
    m <- auto.adam(x, h = h)
    pred <- m$forecast
    pred
  }
  
  
  sma_forecast <<- function(x, h){

    m <- sma(y = x, 
             h = h,
             ic = "AICc")
    pred <- m$forecast
    pred
  }
  
  arima_aug <<- function(x, h, dates) {
    historic_aug <<- (dates %in% setdiff(seq.Date("2015-08-01" %>% as.Date(), "2027-10-01" %>% as.Date(), by = "year"), "2022-08-01" %>% as.Date())) %>% as.numeric()
    historic_covid <<- (dates %in% seq.Date("2020-04-01" %>% as.Date(), "2020-06-01" %>% as.Date(), by = "month")) %>% as.numeric()
    
    
    
    future_dates <- future_dates <- seq.Date(max(dates), length.out = h+1 , by = "month")[-1]
    
    model <- forecast:::auto.arima( x, stepwise = FALSE,
                                    xreg = cbind(historic_aug, historic_covid) )
    
    future_aug <<- (future_dates %in% seq.Date("2015-08-01" %>% as.Date(), "2027-10-01" %>% as.Date(), by = "year")) %>% as.numeric()
    future_covid <<- (future_dates %in% seq.Date("2020-04-01" %>% as.Date(), "2020-06-01" %>% as.Date(), by = "month")) %>% as.numeric()
    
    xregmat <<- cbind(future_aug, future_covid)
    
    
    colnames(xregmat) <- c("historic_aug", "historic_covid") 
    
    print(future_aug)
    print(xregmat)
    
    forecast::forecast( model, h = h,
                        xreg = xregmat )$mean
     
  }
  
  stl_arima <<- function(x, h, dates) {
    historic_aug <<- (dates %in% setdiff(seq.Date("2015-08-01" %>% as.Date(), "2027-10-01" %>% as.Date(), by = "year"), "2022-08-01" %>% as.Date())) %>% as.numeric()
    historic_covid <<- (dates %in% seq.Date("2020-04-01" %>% as.Date(), "2020-06-01" %>% as.Date(), by = "month")) %>% as.numeric()
  
    future_dates <- future_dates <- seq.Date(max(dates), length.out = h+1 , by = "month")[-1]
    
    model <- stlm( x, method = "arima",
                                    xreg = cbind(historic_aug, historic_covid), stepwise = FALSE) #added stepwise = FALSE 
    
    future_aug <<- (future_dates %in% seq.Date("2015-08-01" %>% as.Date(), "2027-10-01" %>% as.Date(), by = "year")) %>% as.numeric()
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
                                                hp = seriesdata$hp),
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
   calc_forecast_seb(lst, append(forecast_strings, c("sma_forecast",                
                                   "arima_aug",
                                   #"prophet_forecast",
                                   "stlf_forecast",
                                   "stl_arima",
                                   "adam_forecast"                  
                                   )), n.cores = 1)
                   
                
}

# COMMAND ----------

create_list_for_spark <- function(lst_data){
  
  nr_workers <- spark_context(sc) %>% invoke("getExecutorMemoryStatus") %>% names() %>% length() - 1
  nr_cores <- 8
  total_cores <- nr_workers * nr_cores
  lst_for_spark <- list()
  subsequences <- split(1:length(lst_data), 1:total_cores)
  for (i in 1:total_cores) {
    lst_for_spark[[i]] <- lst_data[subsequences[[i]]]
  }
  lst_for_spark
}

# COMMAND ----------

forecast_list <- SparkR::spark.lapply(forecast_list %>% create_list_for_spark(), func = calculate_forecast) %>% unlist(recursive = FALSE)

# COMMAND ----------

null_index <- c()
for(i in 1:length(forecast_list)) {
  sm <- forecast_list[[i]]$SalesMarket
  pl <- forecast_list[[i]]$ProductLine
  if(sm == 'MA' & pl == 'Tooling Systems') print(forecast_list[[i]])
}

# COMMAND ----------

ProductLines <- income %>% pull(ProductLine) %>% unique()
light_THA_features <- function(full_lst){
  
  vectorize <- function(lst){
    pl <- lst$ProductLine
    inds <- 1 * (ProductLines %in% c(pl))
    feat_tbl <- matrix(inds, nrow = 1)
    colnames(feat_tbl) <- (ProductLines %>% str_replace(pattern = " ", replace = "_"))
    lst$features <- feat_tbl %>% as_tibble()
    lst
  }
  
  lapply(full_lst, vectorize)
}
#forecast_list <- forecast_list %>% light_THA_features()
forecast_list_raw <- forecast_list
forecast_list <- forecast_list %>% THA_features(n.cores = 15)

# COMMAND ----------

null_index <- c()
for(i in 1:length(forecast_list)) {
  feat <- forecast_list[[i]]$features
  ff <- forecast_list[[i]]$ff
  if(is.null(feat)) null_index <- c(null_index, i)
  if((ff %>% as.vector() %>% is.na() %>% sum() > 0)) null_index <- c(null_index, i)
}
forecast_list[null_index %>% unique()] <- NULL
sma_list <- append(sma_list, forecast_list_raw[null_index %>% unique()])

for(i in 1:length(forecast_list)) {  
  colnames(forecast_list[[i]]$features) <- colnames(forecast_list[[i]]$features) %>% str_replace(pattern="\\.", "_")
}

# COMMAND ----------

for(i in 1:length(forecast_list)){
  ff <- forecast_list[[i]]$ff
  ff[ ff < 0 ] <- 0
  ff[4,] = ff[10,] #tbats messing things up
  forecast_list[[i]]$ff <- ff
}

# COMMAND ----------

library(xgboost)

# COMMAND ----------

path_in <- paste0("/mnt/blob/hyperparameters/fforma/income/current/data_pl_sm_wd_feat.csv")
xgb_data <- spark_read_csv(sc, 
                       path = path_in,
                       delimiter = ";"
                                    ) %>%
  arrange(index) %>%
  collect() %>%
  select(-index) %>%
  as.matrix()

path_in <- paste0("/mnt/blob/hyperparameters/fforma/income/current/errors_pl_sm_wd_feat.csv")
xgb_errors <- spark_read_csv(sc, 
                       path = path_in,
                       delimiter = ";"
                                    ) %>%
  arrange(index) %>%
  collect() %>%
  select(-index) %>%
  as.matrix()

path_in <- paste0("/mnt/blob/hyperparameters/fforma/income/current/xgb_params_pl_sm_wd_feat.csv")
param <- spark_read_csv(sc, 
                       path = path_in,
                       delimiter = ";"
                                    ) %>%
  collect() 

# COMMAND ----------

library(xgboost)

# COMMAND ----------

pred_data <- create_feat_classif_problem(forecast_list)

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

forecast_list <- ensemble_forecast(preds, forecast_list)

# COMMAND ----------

to_tbl <- function(lst, end_date = end_date){
  library(tidyverse)
  library(lubridate)
  pl <- lst$ProductLine
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
         ProductLine = pl,
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

sma_list <- SparkR::spark.lapply(sma_list %>% create_list_for_spark(), 
                                 func = caluclate_forecast_sma) %>% unlist(recursive = FALSE)
sma_list <- ensemble_forecast(matrix(1, nrow = length(sma_list)), sma_list)

# COMMAND ----------

library(parallel)
library(data.table)
forecast_tbl <- rbindlist(lapply(append(forecast_list, sma_list), 
                                 to_tbl, 
                                 end_date = end_date)) %>% as_tibble()

# COMMAND ----------

forecast_tbl <- forecast_tbl %>%
  left_join(wds %>%
             rename(Date = DateMonth)) %>%
  mutate(working_days = if_else(working_days %>% is.na(),
                                month_avg_total %>% mean(na.rm = TRUE),
                                working_days)) %>%
  mutate(values = working_days * values)  

# COMMAND ----------

forecast_tbl %>%
  group_by(Date, source) %>%
  summarise(y = sum(values),
            wd = mean(working_days)) %>%
  print(n = 400)

# COMMAND ----------

forecast_tbl %>%
  group_by(source, Date) %>%
  summarise(y = sum(values)) %>%
  mutate(inc = c(1, tail(y, -1) / head(y,-1)),
         month = month(Date)) %>%
  ungroup() %>%
  group_by(source, month) %>%
  summarise(inc = mean(inc)) %>%
  pivot_wider(names_from = source, values_from = inc) %>%
  print(n = 100)

# COMMAND ----------



# COMMAND ----------

forecast_tbl %>%
  group_by(Date, source) %>%
  summarise(y = sum(values)) %>%
  ggplot()+
  geom_line(aes(x = Date, y = y, color = source))

# COMMAND ----------

forecast_tbl %>%
  mutate(year = year(Date)) %>%
  group_by(year) %>%
  summarise(sales = sum(values)) %>%
  mutate(inc = c(1, tail(sales, -1) / head(sales,-1)))

# COMMAND ----------

forecast_spark <- sdf_copy_to(sc, 
                                 forecast_tbl,
                                 overwrite = TRUE)
path_out <- paste0("/mnt/blob/forecast/income/productline/current/pl_sm_wd.csv")
spark_write_csv(forecast_spark, 
                path = path_out, 
                delimiter = ";", 
                mode = "overwrite")
path_out <- paste0("/mnt/blob/forecast/income/productline/archive/", Sys.Date() %>% str_replace_all("-", "_") ,"/pl_sm_wd.csv")
spark_write_csv(forecast_spark, 
                path = path_out, 
                delimiter = ";", 
                mode = "overwrite")
#

# COMMAND ----------

tbl <- tibble() 
Date <- seq.Date(forecast_tbl %>% 
  filter(source != "x") %>% 
  pull(Date) %>%
  min() %>% 
  as.Date(), length.out = 60, by = "month")
  for(lst in forecast_list) {
    
    ff <- lst$ff %>% t() %>% as_tibble()
    pl <- lst$ProductLine
    sm <- lst$SalesMarket
    
    tbl <- tbl %>%
      rbind(cbind(Date, ff, pl, sm))
    
  }

# COMMAND ----------

tbl %>% glimpse()

# COMMAND ----------

tbl_piv <- tbl %>%
  filter(sm == "MA", pl == "Tooling Systems") %>%
  select(-c(sm, pl)) %>%
  pivot_longer(-Date, names_to = "source") %>%
  group_by(Date, source) %>%
  summarise(values = sum(value)) %>%
  ungroup()

# COMMAND ----------

tbl_piv %>%
    rbind(income %>%
         rename(Date = DateMonth) %>%
         group_by(Date) %>%
         summarise(values = sum(TotalOrders),
                   source = "x")) %>%
filter(!(source %in% c("x"))) %>%
print(n = 1000)

# COMMAND ----------

tbl_piv %>%
    rbind(income %>%
         rename(Date = DateMonth) %>%
         group_by(Date) %>%
         summarise(values = sum(TotalOrders),
                   source = "x")) %>%
  filter(!(source %in% c("x"))) %>%
  ggplot()+
  geom_line(aes(x = Date, y = values, color = source))
  #scale_x_date(date_breaks = "2 month", date_labels = "%m")