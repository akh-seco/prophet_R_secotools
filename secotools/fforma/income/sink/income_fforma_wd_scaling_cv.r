# Databricks notebook source
# MAGIC %run /Repos/secotools/pipelines/income_read_productline

# COMMAND ----------

income <- income %>%
  filter(complete.cases(.)) %>%
  mutate(guesstimate = guesstimate / weighted_wd)

# COMMAND ----------

path_in <- paste0("/mnt/blob/hyperparameters/prophet/income/current/prophet_hp.csv")
hp_prophet <- spark_read_csv(sc, 
                       path = path_in,
                       delimiter = ";"
                                   ) %>%
  collect() %>%
  filter(scaled == 1, guess == 0, august == 1) %>%
  group_by(ProductLine) %>%
  filter(mape == min(mape)) %>%
  arrange(mape)

# COMMAND ----------

library(M4metalearning)
library(forecast)
create_temporal_list <- function(data){
  to_list <- function(tbl){
    pl <- tbl %>% pull(ProductLine) %>% unique()
    cut_off <- tbl %>% pull(cut_off) %>% unique()
    
    if (cut_off != 0) {
      tbl <- tbl %>%
        arrange(DateMonth) %>%
        head(-cut_off)
    }
    
    h <- 2
    dates <- tbl %>% pull(DateMonth) %>% head(-h)
    values <- ts(tbl %>% pull(TotalOrders_scaled), frequency = 12)
    
    list(
      ProductLine = pl,
      x = values,
      values_check = values,
      h = h,
      Date = dates,
      cut_off = cut_off,
      hp = hp_prophet %>% filter(ProductLine == pl),
      guesstimate = tbl %>% pull(guesstimate),
      wd = tbl %>% pull(weighted_wd)
    )
  }
  cut_off  <- seq(0, 14)
  data %>%
    crossing(cut_off) %>%
    group_split(ProductLine, cut_off) %>%
    lapply(., to_list)
}

temporal_list <- create_temporal_list(income)
temporal_list <- temp_holdout(temporal_list)

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
 
  prophet_forecast <<- function(x, dates, h, hp, g){
    library(tidyverse)
    library(prophet)

    cp <- hp %>% pull(cps)
    sp <- hp %>% pull(sps)
    sm <- hp %>% pull(sm)
    cov <- hp %>% pull(covid)
    fo <- hp %>% pull(fo)
    aug <- hp %>% pull(august)
    guess <- hp %>% pull(guess)

    future_dates <- seq.Date(max(dates), length.out = h+1 , by = "month")[-1]


    prophet_data <- tibble(
       ds = seq.Date(from = "2015-01-01" %>% as.Date(), length.out = (x %>% length()), by = "day"),
       y =  x %>% as.numeric(),
       checksum = dates,
       guesstimate = g %>% head(-h) %>% as.numeric()
      ) %>%
        mutate(covid_ind = checksum %in% seq.Date("2020-04-01" %>% as.Date(), "2020-08-01" %>% as.Date(), by = "month"),
               aug_ind = checksum %in% seq.Date("2018-08-01" %>% as.Date(), "2022-10-01" %>% as.Date(), by = "year"))


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

    if ( cov == 1 ) m <-   add_regressor(m, "covid_ind", standardize = FALSE)
    if ( aug == 1 ) m <-   add_regressor(m, "aug_ind", standardize = FALSE)
    if ( guess == 1 ) m <- add_regressor(m, "guesstimate", standardize = FALSE)

    m <- fit.prophet(m,
           df = prophet_data
          )
    future_points <- make_future_dataframe(m, 
                                            periods = h, 
                                            freq = "day",
                                            include_history = FALSE)
     future_points <- future_points %>%
        mutate(covid_ind = (future_dates %in% seq.Date("2020-04-01" %>% as.Date(), "2020-08-01" %>% as.Date(), by = "month")) %>% as.numeric(),
               aug_ind = (future_dates %in% seq.Date("2018-08-01" %>% as.Date(), "2021-10-01" %>% as.Date(), by = "year")) %>% as.numeric(),
               guesstimate = g %>% tail(h))

    
     pred <- predict(m, future_points)
     pred$yhat
  }
  
 adam_forecast <<- function(x, h){
    
    m <- auto.adam(x, h = h)
    pred <- m$forecast
    pred
   
  }
  
  sma_forecast <<- function(x, h){
    print("here")
    m <- sma(y = x, 
             h = h,
             ic = "AICc", 
             silent = "all")
    pred <- m$forecast
    pred
  }
  
  arima_aug <<- function(x, h, dates, g) {
    historic_aug <<- (dates %in% seq.Date("2015-08-01" %>% as.Date(), "2021-10-01" %>% as.Date(), by = "year")) %>% as.numeric()
    historic_covid <<- (dates %in% seq.Date("2020-04-01" %>% as.Date(), "2020-06-01" %>% as.Date(), by = "month")) %>% as.numeric()
    
    
    
    future_dates <- future_dates <- seq.Date(max(dates), length.out = h+1 , by = "month")[-1]
    
    model <- forecast:::auto.arima( x, stepwise = FALSE,
                                    xreg = cbind(historic_aug, historic_covid) )
    
    future_aug <<- (future_dates %in% seq.Date("2015-08-01" %>% as.Date(), "2021-10-01" %>% as.Date(), by = "year")) %>% as.numeric()
    future_covid <<- (future_dates %in% seq.Date("2020-04-01" %>% as.Date(), "2020-06-01" %>% as.Date(), by = "month")) %>% as.numeric()
    
    xregmat <<- cbind(future_aug, future_covid)
    
    
    colnames(xregmat) <- c("historic_aug", "historic_covid")
    
    print(future_aug)
    print(xregmat)
    
    forecast::forecast( model, h = h,
                        xreg = xregmat )$mean
     
  }
  
  stl_arima <<- function(x, h, dates, g) {
    historic_aug <<- (dates %in% seq.Date("2015-08-01" %>% as.Date(), "2021-10-01" %>% as.Date(), by = "year")) %>% as.numeric()
    historic_covid <<- (dates %in% seq.Date("2020-04-01" %>% as.Date(), "2020-06-01" %>% as.Date(), by = "month")) %>% as.numeric()
  
    future_dates <- future_dates <- seq.Date(max(dates), length.out = h+1 , by = "month")[-1]
    
    model <- stlm( x, method = "arima",
                                    xreg = cbind(historic_aug, historic_covid), stepwise = FALSE) #added stepwise = FALSE 
    
    future_aug <<- (future_dates %in% seq.Date("2015-08-01" %>% as.Date(), "2021-10-01" %>% as.Date(), by = "year")) %>% as.numeric()
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
                                                dates = seriesdata$Date,
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
   calc_forecast_seb(lst, append(forecast_strings, c("sma_forecast",                
                                   "arima_aug",
                                   "prophet_forecast",
                                   "stlf_forecast",
                                   "stl_arima",
                                   "adam_forecast"                  
                                   )), n.cores = 1)
                   
                
}

# COMMAND ----------

create_list_for_spark <- function(lst_data){
  
  nr_workers <- spark_context(sc) %>% invoke("getExecutorMemoryStatus") %>% names() %>% length() - 1
  nr_cores <- 8
  total_cores <- nr_cores * nr_workers
  lst_for_spark <- list()
  subsequences <- split(1:length(lst_data), 1:total_cores)
  for (i in 1:total_cores) {
    lst_for_spark[[i]] <- lst_data[subsequences[[i]]]
  }
  lst_for_spark
}

# COMMAND ----------

forecast_list <- SparkR::spark.lapply(temporal_list %>% create_list_for_spark(), func = calculate_forecast) %>% unlist(recursive = FALSE)

# COMMAND ----------

mae_errors_lst <- list()
i <- 1
for(i in 1:length(forecast_list)){
  
  mae_errors_lst[[i]] <- sweep(forecast_list[[i]]$ff, 2, forecast_list[[i]]$xx) %>% abs() %>% t() %>% colMeans()
  
}
mae_errors <- mae_errors_lst[[1]]
for(i in 2:length(mae_errors_lst)){
  mae_errors <- mae_errors %>%
    rbind(mae_errors_lst[[i]])
}
mae_errors %>%
  colSums() %>%
  sort()

# COMMAND ----------

for (i in 1:length(forecast_list)){
  ff <- forecast_list[[i]]$ff
  ff[ ff < 0 ] <- 0
  forecast_list[[i]]$ff <- ff
}

# COMMAND ----------

 unlockBinding("calc_errors", as.environment("package:M4metalearning"))
  assignInNamespace("calc_errors", 
                    function (dataset) 
{
  total_snaive_errors <- c(0, 0)
  for (i in 1:length(dataset)) {
    tryCatch({
      lentry <- dataset[[i]]
      insample <- lentry$x
      ff <- lentry$ff
      ff <- rbind(ff, snaive_forec(insample, lentry$h))
      frq <- frq <- stats::frequency(insample)
      insample <- as.numeric(insample)
      outsample <- as.numeric(lentry$xx)
      masep <- mean(abs(utils::head(insample, -frq) - 
        utils::tail(insample, -frq)))
      repoutsample <- matrix(rep(outsample, each = nrow(ff)), 
        nrow = nrow(ff))
      smape_err <- abs(ff - repoutsample)
      mase_err <- abs(ff - repoutsample)
      
      lentry$snaive_mase <- mase_err[nrow(mase_err), ]
      lentry$snaive_smape <- smape_err[nrow(smape_err), 
        ]
      lentry$mase_err <- mase_err[-nrow(mase_err), ]
      lentry$smape_err <- smape_err[-nrow(smape_err), ]
      dataset[[i]] <- lentry
      total_snaive_errors <- total_snaive_errors + c(mean(lentry$snaive_mase), 
        mean(lentry$snaive_smape))
    }, error = function(e) {
      print(paste("Error when processing OWIs in series: ", 
        i))
      print(e)
      e
    })
  }
  total_snaive_errors = total_snaive_errors/length(dataset)
  avg_snaive_errors <- list(avg_mase = total_snaive_errors[1], 
    avg_smape = total_snaive_errors[2])
  for (i in 1:length(dataset)) {
    lentry <- dataset[[i]]
    dataset[[i]]$errors <- rowMeans(lentry$mase_err)
  }
  attr(dataset, "avg_snaive_errors") <- avg_snaive_errors
  dataset
},
                    ns = "M4metalearning", 
                    envir = as.environment("package:M4metalearning"))
  assign("calc_errors", 
         function (dataset) 
{
  total_snaive_errors <- c(0, 0)
  for (i in 1:length(dataset)) {
    tryCatch({
      lentry <- dataset[[i]]
      insample <- lentry$x
      ff <- lentry$ff
      ff <- rbind(ff, snaive_forec(insample, lentry$h))
      frq <- frq <- stats::frequency(insample)
      insample <- as.numeric(insample)
      outsample <- as.numeric(lentry$xx)
      masep <- mean(abs(utils::head(insample, -frq) - 
        utils::tail(insample, -frq)))
      repoutsample <- matrix(rep(outsample, each = nrow(ff)), 
        nrow = nrow(ff))
      smape_err <- abs(ff - repoutsample)
      mase_err <- abs(ff - repoutsample)
      
      lentry$snaive_mase <- mase_err[nrow(mase_err), ]
      lentry$snaive_smape <- smape_err[nrow(smape_err), 
        ]
      lentry$mase_err <- mase_err[-nrow(mase_err), ]
      lentry$smape_err <- smape_err[-nrow(smape_err), ]
      dataset[[i]] <- lentry
      total_snaive_errors <- total_snaive_errors + c(mean(lentry$snaive_mase), 
        mean(lentry$snaive_smape))
    }, error = function(e) {
      print(paste("Error when processing OWIs in series: ", 
        i))
      print(e)
      e
    })
  }
  total_snaive_errors = total_snaive_errors/length(dataset)
  avg_snaive_errors <- list(avg_mase = total_snaive_errors[1], 
    avg_smape = total_snaive_errors[2])
  for (i in 1:length(dataset)) {
    lentry <- dataset[[i]]
    dataset[[i]]$errors <- rowMeans(lentry$mase_err)
  }
  attr(dataset, "avg_snaive_errors") <- avg_snaive_errors
  dataset
}, 
         envir = as.environment("package:M4metalearning"))

# COMMAND ----------

forecast_list <- calc_errors(forecast_list)

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
forecast_list <- forecast_list %>% light_THA_features()

# COMMAND ----------

install.packages("rBayesianOptimization")

# COMMAND ----------

library(xgboost)
hp_search <- function (dataset, filename = "M4_hyper.RData", n_iter = 1000, 
  n.cores = 15) 
{
  N_THREAD = n.cores
  whole_dataset <- dataset
  folds <- rBayesianOptimization::KFold(1:length(whole_dataset), 
    nfolds = 5, seed = 31 - 5 - 2018)
  train_ds <- NULL
  test_ds <- NULL
  train_feat <- NULL
  test_feat <- NULL
  for (i in 1:length(folds)) {
    train_ds[[i]] <- whole_dataset[-folds[[i]]]
    train_feat[[i]] <- create_feat_classif_problem(train_ds[[i]])
    test_ds[[i]] <- whole_dataset[folds[[i]]]
    test_feat[[i]] <- create_feat_classif_problem(test_ds[[i]])
  }
  bayes_xgb <- function(max_depth, eta, subsample, colsample_bytree, 
    nrounds) {
    n_class <- nrow(train_ds[[1]][[1]]$ff)
    bay_results <- NULL
    param <- list(max_depth = max_depth, eta = eta, nthread = N_THREAD, 
      silent = 0, objective = error_softmax_obj, num_class = n_class, 
      subsample = subsample, colsample_bytree = colsample_bytree)
    final_error = NULL
    final_preds = NULL
    for (i in 1:length(folds)) {
      dtrain <- xgboost::xgb.DMatrix(train_feat[[i]]$data)
      attr(dtrain, "errors") <- train_feat[[i]]$errors
      bst <- xgboost::xgb.train(param, dtrain, nrounds)
      preds <- M4metalearning::predict_selection_ensemble(bst, 
        test_feat[[i]]$data)
      er <- M4metalearning::summary_performance(preds, 
        test_ds[[i]], print.summary = FALSE)
      final_error <- c(final_error, er$weighted_error)
      final_preds <- rbind(final_preds, preds)
    }
    bay_results <- rbind(bay_results, c(max_depth, eta, 
      subsample, colsample_bytree, nrounds, mean(final_error)))
    try({
      colnames(bay_results) <- c("max_depth", "eta", "subsample", 
        "colsample_bytree", "nrounds", "combi_OWA")
    })
    bay_results <- as.data.frame(bay_results)
    save(bay_results, file = filename)
    list(Score = -mean(final_error), Pred = final_preds)
  }
  prefound_grid <- list(max_depth = 10L, eta = 0.4, subsample = 0.9, 
    colsample_bytree = 0.6, nrounds = 200)
  k = 2
  bay_res <- rBayesianOptimization::BayesianOptimization(bayes_xgb, 
    bounds = list(max_depth = c(6L, 14L), eta = c(0.001, 
      1), subsample = c(0.5, 1), colsample_bytree = c(0.5, 1), nrounds = c(1L, 250L)), init_grid_dt = prefound_grid, 
    init_points = 5, kappa = 2.576, n_iter = n_iter, kernel = list(type = "matern", 
      nu = (2 * k + 1)/2))
  bay_res
}

# COMMAND ----------

hyper <- hp_search(forecast_list, filename = "tmp.RData", n_iter = 30)

# COMMAND ----------

p <- hyper$History %>% 
  filter(Value == max(Value))

# COMMAND ----------

train_data <- create_feat_classif_problem(forecast_list)
param <- list(max_depth = p$max_depth,
              eta = p$eta,
              nthread = 3,
              nrounds = p$nrounds,
              silent=1,
              objective=error_softmax_obj,
              num_class=ncol(train_data$errors), #the number of forecast methods used
              subsample =  p$subsample,
              colsample_bytree = p$colsample_bytree)


meta_model <- train_selection_ensemble(train_data$data,
                                       train_data$errors,
                                       param = param)
preds <- predict_selection_ensemble(meta_model, train_data$data)

# COMMAND ----------

param$objective <- NULL
param_spark <- sdf_copy_to(sc, param %>% unlist(recursive = FALSE) %>% as.matrix(nrow = 1, ncol = param %>% length()) %>% t() %>% as_tibble(), overwrite = TRUE)
path_out <- paste0("/mnt/blob/hyperparameters/fforma/income/current/xgb_params_wd.csv")
spark_write_csv(param_spark, 
                path = path_out, 
                delimiter = ";", 
                mode = "overwrite")

# COMMAND ----------

param_spark

# COMMAND ----------

xgb_data <- train_data$data %>% as_tibble() %>% mutate(index = 1:nrow(.))
xgb_data_spark <- sdf_copy_to(sc, 
                                 xgb_data,
                                 overwrite = TRUE)
path_out <- paste0("/mnt/blob/hyperparameters/fforma/income/current/data_wd_scaling.csv")
spark_write_csv(xgb_data_spark, 
                path = path_out, 
                delimiter = ";", 
                mode = "overwrite")

xgb_errors <- train_data$errors %>% as_tibble() %>% mutate(index = 1:nrow(.))
xgb_errors_spark <- sdf_copy_to(sc, 
                                 xgb_errors,
                                 overwrite = TRUE)
path_out <- paste0("/mnt/blob/hyperparameters/fforma/income/current/errors_wd_scaling.csv")
spark_write_csv(xgb_errors_spark, 
                path = path_out, 
                delimiter = ";", 
                mode = "overwrite")

# COMMAND ----------

for (i in 1:length(forecast_list)){
  
  forecast_list[[i]]$xx <- forecast_list[[i]]$xx * tail(forecast_list[[i]]$wd, 2)
  forecast_list[[i]]$ff <- sweep(forecast_list[[i]]$ff, 2, tail(forecast_list[[i]]$wd, 2), "*")
  ff <- forecast_list[[i]]$ff
  ff[ ff < 0 ] <- 0
  forecast_list[[i]]$ff <- ff
  
}

# COMMAND ----------

forecast_list <- ensemble_forecast(preds, forecast_list)

# COMMAND ----------

errors <- forecast_list[[1]]$errors
errors_yhat <- mean(abs(forecast_list[[1]]$y_hat - forecast_list[[1]]$xx))
periods <- 1
for(lst in forecast_list[-1]) {
  periods <- periods + 1
  errors <- errors + lst$errors
  errors_yhat <- errors_yhat + mean(abs(lst$y_hat - lst$xx))
}
(c(errors, y_hat = errors_yhat) / periods) %>% sort()

# COMMAND ----------

summary_performance(preds, dataset = forecast_list)

# COMMAND ----------

mat <- xgboost::xgb.importance (feature_names = colnames(train_data$data),
                       model = meta_model)
xgboost::xgb.plot.importance(importance_matrix = mat[1:20], cex=1.0)

# COMMAND ----------

tbl <- tibble(DateMonth = Date(), 
              ProductLine = character(),
              values = numeric(),
              source = character())

# COMMAND ----------

tbl <- tibble(DateMonth = Date(), 
              ProductLine = character(),
              values = numeric(),
              source = character())

for(i in 1:length(forecast_list)) {
  pl <- forecast_list[[i]]$ProductLine
  date <- forecast_list[[i]]$Date %>% tail(1) %m+% months(1)
  TotalOrders <- forecast_list[[i]]$xx %>% head(1) %>% as.numeric()
  forecast <- forecast_list[[i]]$y_hat %>% head(1)
  forecast_ff <- forecast_list[[i]]$ff[,1]
  mean_f <- forecast_list[[i]]$ff[,1] %>% mean()
  
   tbl <- tbl %>%
     add_row(DateMonth = date, 
                  ProductLine = pl,  
                  values = c(TotalOrders, mean_f, forecast, forecast_ff),
                  source = c("TotalOrders", "mean_forecast", "y_hat", forecast_list[[i]]$ff %>% rownames()))
}

# COMMAND ----------

tbl %>%
  group_by(DateMonth, source) %>%
  summarise(values = sum(values)) %>%
  ggplot()+
  geom_line(aes(x = DateMonth, y = values, color = source))

# COMMAND ----------

tbl %>%
  filter(source %in% c("TotalOrders", source[15])) %>%
  group_by(DateMonth, source) %>%
  summarise(values = sum(values)) %>%
  ggplot()+
  geom_line(aes(x = DateMonth, y = values, color = source))

# COMMAND ----------

colnames(preds) <- forecast_list[[1]]$ff %>% rownames()

# COMMAND ----------

preds %>% colSums()

# COMMAND ----------

tbl %>%
  filter(source %in% c("TotalOrders", "y_hat")) %>%
  group_by(DateMonth, source) %>%
  summarise(values = sum(values)) %>%
  print(n = 100)

# COMMAND ----------

tbl %>%
  filter(source %in% c("TotalOrders", "y_hat")) %>%
  group_by(DateMonth, source) %>%
  summarise(values = sum(values)) %>%
  ggplot()+
  geom_line(aes(x = DateMonth, y = values, color = source))

# COMMAND ----------

tbl %>%
  pivot_wider(names_from = source, values_from = values) %>%
  pivot_longer(-c(DateMonth, ProductLine, TotalOrders), names_to = "source", values_to = "values") %>%
  group_by(DateMonth, source) %>%
  summarise(mape = abs( sum(TotalOrders)-sum(values) ) / sum(TotalOrders),
            mae = abs( sum(TotalOrders)-sum(values) )) %>%
  ungroup() %>%
  group_by(source) %>%
  summarise(mape = mean(mape),
            mae = mean(mae)) %>%
  arrange(mape)


