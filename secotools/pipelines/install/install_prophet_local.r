# Databricks notebook source
library(tidyverse)

# COMMAND ----------

#Install on workers
library(sparklyr)
sc <- spark_connect(method = "databricks")

nr_workers <- spark_context(sc) %>% invoke("getExecutorMemoryStatus") %>% names() %>% length() - 1
print(nr_workers)

SparkR::spark.lapply(seq(1,nr_workers), function(x){
#remove.packages('rstantools')
devtools::install_version("rstantools", version = "2.0.0", repos = "https://mran.revolutionanalytics.com/snapshot/2021-08-15") 
Sys.setenv(DOWNLOAD_STATIC_LIBV8 = 1)
remotes::install_github("jeroen/V8", repos = "https://mran.revolutionanalytics.com/snapshot/2021-08-15")
install.packages("prophet", repos = "https://mran.revolutionanalytics.com/snapshot/2021-08-15")
}
                     )

# COMMAND ----------

prophet_init <- function(x) {
    library(prophet)
    library(tidyverse)
    prophet_data <- tibble(ds = seq.Date(from = "2015-01-01" %>% as.Date(), length.out = x %>% length(), by = "day"),
                           y = x)
    m <- prophet(
          daily.seasonality  = FALSE,
          yearly.seasonality = FALSE,
          weekly.seasonality = FALSE)
 
    m <- fit.prophet(m, df = prophet_data)
    future_points <- make_future_dataframe(m, 
                                           periods = 12,
                                           freq = "day",
                                           include_history = FALSE)
    pred <- predict(m, future_points)$yhat 
    pred
    }
nr_workers <- spark_context(sc) %>% invoke("getExecutorMemoryStatus") %>% names() %>% length() - 1 #-1 = the driver
nr_cores <- 8
total_cores <-  nr_cores * nr_workers
tmp <- function(x){
  rnorm(n = 40)
}
y <- lapply(1:total_cores, tmp)
SparkR::spark.lapply(y, prophet_init)

# COMMAND ----------

install.packages("forecast")


