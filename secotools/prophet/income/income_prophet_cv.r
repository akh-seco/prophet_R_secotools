# Databricks notebook source
# MAGIC %run /Repos/secotools/pipelines/income_read_productline

# COMMAND ----------

library(lubridate)

# COMMAND ----------

income <- income %>%
  filter(complete.cases(.))

# COMMAND ----------

cps <- seq(0.001, 0.5, length.out = 10)
sps <- seq(0.01, 10, length.out = 10)
sm <- c("additive", "multiplicative")
aug <- c(0, 1)
scaled <- c(0, 1)
covid <- c(0, 1)
guess <- c(0,1)
fourier_order <- c(10, 15)

hp_grid <- tibble(cps = cps) %>%
  crossing(sps) %>%
  crossing(sm) %>%
  crossing(covid) %>%
  crossing(aug) %>%
  crossing(fourier_order) %>%
  crossing(scaled) %>%
  crossing(guess)

tmp_list <- hp_grid %>%
  group_split(cps, sps, sm, covid, aug, fourier_order, scaled, guess)

# COMMAND ----------

hp_list <- list()
i <- 1
for(lst in tmp_list) {
  slice <- list(hp = lst, prophet_data = income %>% filter(DateMonth <= "2021-06-01"))
  hp_list[[i]] <- slice
  i <- i+1
}



# COMMAND ----------

prophet_cv <- function(lst){
  library(tidyverse)
  library(lubridate)
  library(prophet)
  
  pl_cv <- function(data, hp_tbl){
    
    
    cp <- hp_tbl %>% pull(cps)
    sp <- hp_tbl %>% pull(sps)
    sm <- hp_tbl %>% pull(sm)
    cov <- hp_tbl %>% pull(covid)
    aug <- hp_tbl %>% pull(aug)
    fo <- hp_tbl %>% pull(fourier_order)
    scaled <- hp_tbl %>% pull(scaled)
    guess <- hp_tbl %>% pull(guess)
    
    pl <- data %>% pull(ProductLine) %>% unique()   
    
    print(pl)
    

    data <- data %>%
      arrange(DateMonth)
    
    
    
    values <- ts(data %>% pull(TotalOrders), frequency = 12) 
    guesstimate_values <- data %>% pull(guesstimate)
    if( scaled == 1 ) {
      values <- ts(data %>% pull(TotalOrders_scaled), frequency = 12)
      guesstimate_values <- (data %>% pull(guesstimate)) / (data %>% pull(weighted_wd))  
    }
     
    prophet_data <- tibble(
      ds = seq.Date(from = "2015-01-01" %>% as.Date(), length.out = (values %>% length()), by = "day"),
      y =  values %>% as.numeric(),
      checksum = data %>% pull(DateMonth),
      guesstimate = guesstimate_values
    ) %>%
      mutate(covid_ind = checksum %in% seq.Date("2020-04-01" %>% as.Date(), "2020-08-01" %>% as.Date(), by = "month"),
             aug_ind = checksum %in% seq.Date("2018-08-01" %>% as.Date(), "2022-10-01" %>% as.Date(), by = "year")) #N.B.

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
  if ( guess == 1 ) m <- add_regressor(m, "guesstimate", standardize = FALSE) 
  
   m <- fit.prophet(m,
          df = prophet_data
         )

  cv <- cross_validation(m,
    horizon = 3,
    units = "days",
    period = 1,
    initial = floor( 0.84 * length(values))
  )
    
 
  
  tibble(ProductLine = pl, 
         cps = cp,
         sps = sp,
         sm = sm,
         covid = cov,
         august = aug,
         fo = fo,
         guess = guess,
         scaled = scaled,
         mape = performance_metrics(cv)$mape %>% mean(),
         mae = performance_metrics(cv)$mae %>% mean())
    
  }
  
  pl_data <- lst$prophet_data
  hp <- lst$hp
  pl_data %>%
    group_by(ProductLine) %>%
    do(pl_cv(., hp_tbl = hp)) %>%
    ungroup()
}

# COMMAND ----------

cv <- SparkR::spark.lapply(hp_list, prophet_cv)

# COMMAND ----------

list_to_tbl <- function(full_lst){
  tbl <- c()
  for(lst in full_lst) tbl <- tbl %>% rbind(lst)
    tbl 
}
cv_data <- list_to_tbl(cv)

# COMMAND ----------

cv_data_spark <- sdf_copy_to(sc, 
                             cv_data,
                             overwrite = TRUE)

# COMMAND ----------

path_out <- paste0("/mnt/blob/hyperparameters/prophet/income/current/prophet_hp.csv")
spark_write_csv(cv_data_spark, 
                path = path_out, 
                delimiter = ";", 
                mode = "overwrite")

# COMMAND ----------

path_out <- paste0("/mnt/blob/hyperparameters/prophet/income/archive/prophet_hp_", Sys.Date(), ".csv")
spark_write_csv(cv_data_spark, 
                path = path_out, 
                delimiter = ";", 
                mode = "overwrite")

# COMMAND ----------

cv_data %>%
  group_by(ProductLine) %>%
  filter(mape == min(mape)) %>%
  arrange(mape)


