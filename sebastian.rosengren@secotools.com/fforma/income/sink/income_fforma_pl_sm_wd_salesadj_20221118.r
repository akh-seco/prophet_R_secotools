# Databricks notebook source
# MAGIC %run /Repos/sebastian.rosengren@secotools.com/pipelines/mount_datalake

# COMMAND ----------

path_in <- paste0("/mnt/blob/forecast/income/productline/current/pl_sm_wd.csv")
pl_forecast_sm <- spark_read_csv(sc, 
                       path = path_in,
                       delimiter = ";"
                                   ) %>%
  collect() %>%
  mutate(Date = Date %>% as.Date()) %>%
  select(Date, 
         ProductLine, 
         SalesMarket,
         values,
         source) 

# COMMAND ----------

pl_forecast_sm %>%
  #filter(SalesMarket == "JA") %>%
  group_by(ProductLine, SalesMarket) %>%
  summarise(md = min(Date), y = sum(values)) %>%
  ungroup() %>%
  arrange(md) %>%
  print(n = 400)

# COMMAND ----------

end_date <- pl_forecast_sm %>%
  filter(source == "x") %>%
  pull(Date) %>%
  max()

impute_dates <- function(tbl){
  
      pl <- tbl %>% pull(ProductLine) %>% unique()
      sm <- tbl %>% pull(SalesMarket) %>% unique()
      s <- tbl %>% pull(source) %>% unique()
  
      tbl %>% 
        tidyr::complete(Date = seq.Date(min(Date), end_date, by = "month")) %>% 
        mutate(values = replace(values, is.na(values), 0),
               ProductLine = pl,
               SalesMarket = sm,
               source = s)
  
    }

# COMMAND ----------

pl_forecast_sm_x <- pl_forecast_sm %>%
  filter(source == "x") %>%
  group_by(SalesMarket, ProductLine) %>%
  do(impute_dates(.)) %>%
  ungroup()

# COMMAND ----------

pl_forecast_sm <- pl_forecast_sm_x %>%
  rbind(pl_forecast_sm %>%
         filter(source == "yhat"))

# COMMAND ----------

path_in <- paste0("/mnt/blob/utils/SalesRegions.csv")
SalesRegions <- spark_read_csv(sc, 
                       path = path_in,
                       delimiter = ";"
                                   ) %>%
  collect() %>%
  rename(SalesMarket = Company_id) %>%
  select(SalesMarket, Sales_Region)

# COMMAND ----------

pl_forecast_sm <- pl_forecast_sm %>%
  left_join(SalesRegions) %>%
  mutate(Sales_Region = ifelse(Sales_Region %>% is.na(), "Total", Sales_Region)) 

# COMMAND ----------

#SOP volume forecast 2022-2025_220919os.csv

path_in <- paste0("/mnt/blob/utils/SOP volume forecast 2022-2025_220919os.csv") #Updated Qrtly
sales_input <- spark_read_csv(sc, 
                       path = path_in,
                       delimiter = ";"
                                   ) %>%
  collect() %>%
  pivot_longer(-c(Sales_Region, ProductLine), names_to = "year", values_to = "increase") %>%
  mutate(year = year %>% as.numeric,
         increase = (increase + 100) / 100)

# COMMAND ----------



# COMMAND ----------

sales_input <- sales_input %>%
  left_join(pl_forecast_sm %>%
  group_by(SalesMarket, Sales_Region) %>%
  tally() %>%
  select(-n))

# COMMAND ----------

pl_forecast_sm <- pl_forecast_sm %>%
  mutate(month = month(Date), year = year(Date)) %>%
  filter(year <= (sales_input %>% pull(year) %>% max()))

# COMMAND ----------

library(lubridate)
prev_dec <<- 0
apply_sales_adj <- function(tbl) {
  pl <- tbl %>% pull(ProductLine) %>% unique()
  sm <- tbl %>% pull(SalesMarket) %>% unique()
  
  this_year <- tbl %>% pull(year) %>% unique()
  last_year <- this_year - 1
  
  print(c(pl, sm, this_year, last_year))
  
  sales <- tbl %>%
    pull(values) %>%
    sum()
    
  if( this_year <= 2021 || pl == "MISSING" ) {
    print("here")
    prev_dec <<- tbl %>%
      filter(month == 12) %>%
      pull(values)
    return( tbl )
  } 
  else if( this_year == 2022 ) {
    
  prev_dec <<- tbl %>%
      filter(month == 12) %>%
      pull(values)
    
  current_month <- tbl %>%
    filter(source == "x") %>%
    pull(month) %>%
    max()
  
    
  current_sales <- tbl %>%
    filter(month < current_month) %>%
    pull(values) %>%
    sum()
  
  print(current_sales)
    
  increase <- sales_input %>%
    filter(ProductLine == pl, 
           SalesMarket == sm, 
           year <= this_year) %>%
    pull(increase)
  
  first_year_sales <- pl_forecast_sm %>%
    filter(ProductLine == pl, SalesMarket == sm, year == 2021) %>%
    pull(values) %>%
    sum()
  
  target <- prod(increase) * first_year_sales - current_sales 
  
  unaffected <- pl_forecast_sm %>%
    filter(ProductLine == pl, SalesMarket == sm, year == this_year) %>%
    filter(month < current_month)
  
  affected <- pl_forecast_sm %>%
    filter(ProductLine == pl, SalesMarket == sm, year == this_year) %>%
    filter(month >= current_month) 
    
  current_this_year <- pl_forecast_sm %>%
    filter(ProductLine == pl, SalesMarket == sm, year == this_year, month == current_month) %>%
    pull(values) 
    
  last_this_year <- pl_forecast_sm %>%
    filter(ProductLine == pl, SalesMarket == sm, year == this_year, month == 12) %>%
    pull(values) 
      
  affected <- affected %>%
    mutate(L = (month-current_month) * (last_this_year-current_this_year) / (12-current_month) + current_this_year) %>%
    mutate(eps = values-L) %>%
    mutate(values = (month-current_month) * ((12-current_month) * (target - sum(eps)-(12-current_month+1)*current_this_year) / (sum(month-current_month+1)-(12-current_month+1)) ) / (12-current_month) + current_this_year + eps) %>%
    select(-c(L, eps))    
    
  #this_year_tbl <- this_year_tbl %>%
    #mutate(L = (month-1) * (dec_this_year-jan_this_year) / 11 + jan_this_year) %>%
    #mutate(eps = values-L) %>%
    #mutate(values = (month-1) * (11 * (target - sum(eps)-12*jan_this_year) / (sum(month)-12) + jan_this_year-jan_this_year) / 11 + jan_this_year + eps) %>%
    #select(-c(L, eps))
    
    
    
   this_year_tbl <- unaffected %>%
      rbind(affected)
     
   prev_dec <<- this_year_tbl %>%
    filter(month == 12) %>%
    pull(values)
    
   this_year_tbl
    
  }
  else {
  
  increase <- sales_input %>%
    filter(ProductLine == pl, SalesMarket == sm, year <= this_year) %>%
    pull(increase)
  
  first_year_sales <- pl_forecast_sm %>%
    filter(ProductLine == pl, SalesMarket == sm, year == 2021) %>%
    pull(values) %>%
    sum()
  
  target <- prod(increase) * first_year_sales
    
  jan_unadjusted <- pl_forecast_sm %>%
    filter(ProductLine == pl, SalesMarket == sm ,year == this_year, month == 1) %>%
    pull(values)
  
  dec_unadjusted <- pl_forecast_sm %>%
    filter(ProductLine == pl, SalesMarket == sm, year == last_year, month == 12) %>%
    pull(values)  
    
  this_year_tbl <- pl_forecast_sm %>%
    filter(ProductLine == pl, SalesMarket == sm, year == this_year) %>%
    mutate(values = values + (prev_dec-dec_unadjusted))
    
  jan_this_year <- this_year_tbl$values[1]
  dec_this_year <- this_year_tbl$values[12]
  
  this_year_tbl <- this_year_tbl %>%
    mutate(L = (month-1) * (dec_this_year-jan_this_year) / 11 + jan_this_year) %>%
    mutate(eps = values-L) %>%
    mutate(values = (month-1) * (11 * (target - sum(eps)-12*jan_this_year) / (sum(month)-12) + jan_this_year-jan_this_year) / 11 + jan_this_year + eps) %>%
    select(-c(L, eps))
    
  prev_dec <<- this_year_tbl %>%
    filter(month == 12) %>%
    pull(values)
    
  this_year_tbl
    }
}

# COMMAND ----------

adj_forecasts <- pl_forecast_sm %>%
  group_by(ProductLine, SalesMarket, year) %>%
  do(apply_sales_adj(.)) %>%
  ungroup() %>%
  rename(values_adj = values)

# COMMAND ----------

adj_forecasts %>%
  group_by(SalesMarket, ProductLine, year) %>%
  summarise(sales = sum(values_adj)) %>%
  group_by(ProductLine, SalesMarket) %>%
  mutate(inc = 100 * c(1,  tail(sales, -1)/head(sales, -1))) %>%
  arrange(ProductLine, SalesMarket) %>%
  print(n = 600)

# COMMAND ----------

tmp <- adj_forecasts %>%
  left_join(pl_forecast_sm)

# COMMAND ----------

tmp %>%
  group_by(Sales_Region, ProductLine, year) %>%
  summarise(sales = sum(values_adj)) %>%
  group_by(ProductLine, Sales_Region) %>%
  mutate(inc = 100 * c(1,  tail(sales, -1)/head(sales, -1))) %>%
  arrange(ProductLine, Sales_Region) %>%
  print(n = 600)

# COMMAND ----------

adj_forecasts

# COMMAND ----------

pl_forecast_sm

# COMMAND ----------

adj_forecasts

# COMMAND ----------

adj_forecasts <- adj_forecasts %>%
  left_join(pl_forecast_sm) %>%
  select(Date, ProductLine, SalesMarket, values_adj, values, source)

# COMMAND ----------

adj_forecasts %>%
  mutate(year = year(Date)) %>%
  group_by(SalesMarket, ProductLine, year) %>%
  summarise(sales = sum(values_adj)) %>%
  group_by(ProductLine, SalesMarket) %>%
  mutate(inc = 100 * c(1,  tail(sales, -1)/head(sales, -1))) %>%
  arrange(ProductLine, SalesMarket) %>%
  print(n = 600)

# COMMAND ----------

adj_forecasts %>%
  mutate(year = year(Date)) %>%
  group_by(year) %>%
  summarise(y = sum(values),
            y_adj = sum(values_adj))

# COMMAND ----------

forecast_spark <- sdf_copy_to(sc, adj_forecasts,
                                  overwrite = TRUE)
path_out <- paste0("/mnt/blob/forecast/income/productline/current/pl_sm_wd_adj.csv")
spark_write_csv(forecast_spark, 
                path = path_out, 
                delimiter = ";", 
                mode = "overwrite")
path_out <- paste0("/mnt/blob/forecast/income/productline/archive/", Sys.Date() %>% str_replace_all("-", "_") ,"/pl_sm_wd_adj.csv")
spark_write_csv(forecast_spark, 
                path = path_out, 
                delimiter = ";", 
                mode = "overwrite")
