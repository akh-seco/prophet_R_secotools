# Databricks notebook source
# MAGIC %run /Repos/secotools/pipelines/mount_datalake

# COMMAND ----------

path_in <- paste0("/mnt/blob/forecast/invoice/productline/current/pl_sm_wd.csv")
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

pl_forecast <- pl_forecast_sm %>%
  group_by(Date, ProductLine, source) %>%
  summarise(values = sum(values)) %>%
  ungroup()

# COMMAND ----------

pl_forecast %>%
  group_by(Date) %>%
  summarise(y = sum(values)) %>%
  print(n = 144)

# COMMAND ----------

sales_input <- tibble(ProductLine = (pl_forecast %>% pull(ProductLine) %>% unique())) %>%
                      crossing(year = c(2021, 2022, 2023, 2024, 2025, 2026)) %>%
                      mutate(increase = c(1, 1, 1, 1, 1, 1, #Blanks
                                         1, 1.08, 1.07, 1.04, 1.05, 1.05, #Holemaking
                                         1, 1.03, 1.04, 1.02, 1.03, 1.03, #Indexable Milling
                                         1, 1, 1, 1, 1, 1, #MISSING
                                         1, 1.2345, 1.2394, 1.26, 1.2, 1.2, #Other
                                         1, 1.2345, 1.2394, 1.26, 1.2, 1.2, #Services
                                         1, 1.08, 1.08, 1.06, 1.05, 1.05, #Solid Milling
                                         1, 1.03, 1.03, 1.01, 1, 1, #Stationary
                                         1, 1.05, 1.05, 1.04, 1.04, 1.04)) #Tooling Systems

# COMMAND ----------

pl_forecast <- pl_forecast %>%
  mutate(month = month(Date), year = year(Date)) %>%
  filter(year < 2027)

# COMMAND ----------

pl_forecast %>% glimpse()

# COMMAND ----------

library(lubridate)
prev_dec <<- 0
apply_sales_adj <- function(tbl) {
  pl <- tbl %>% pull(ProductLine) %>% unique()
  this_year <- tbl %>% pull(year) %>% unique()
  last_year <- this_year - 1
  
  print(c(pl, this_year, last_year))
  
  if( this_year <= 2021 || pl == "Blanks" || pl == "MISSING") {
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
    filter(ProductLine == pl, year <= this_year) %>%
    pull(increase)
  
  first_year_sales <- pl_forecast %>%
    filter(ProductLine == pl, year == 2021) %>%
    pull(values) %>%
    sum()
  
  target <- prod(increase) * first_year_sales - current_sales 
  
  unaffected <- pl_forecast %>%
    filter(ProductLine == pl, year == this_year) %>%
    filter(month < current_month)
  
  affected <- pl_forecast %>%
    filter(ProductLine == pl, year == this_year) %>%
    filter(month >= current_month) 
    
  current_this_year <- pl_forecast %>%
    filter(ProductLine == pl, year == this_year, month == current_month) %>%
    pull(values) 
    
  last_this_year <- pl_forecast %>%
    filter(ProductLine == pl, year == this_year, month == 12) %>%
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
    filter(ProductLine == pl, year <= this_year) %>%
    pull(increase)
  
  first_year_sales <- pl_forecast %>%
    filter(ProductLine == pl, year == 2021) %>%
    pull(values) %>%
    sum()
  
  target <- prod(increase) * first_year_sales
    
  jan_unadjusted <- pl_forecast %>%
    filter(ProductLine == pl, year == this_year, month == 1) %>%
    pull(values)
  
  dec_unadjusted <- pl_forecast %>%
    filter(ProductLine == pl, year == last_year, month == 12) %>%
    pull(values)  
    
  this_year_tbl <- pl_forecast %>%
    filter(ProductLine == pl, year == this_year) %>%
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

adj_forecasts <- pl_forecast %>%
  group_by(ProductLine, year) %>%
  do(apply_sales_adj(.)) %>%
  ungroup() %>%
  rename(values_adj = values)

# COMMAND ----------

adj_forecasts %>%
  group_by(year, ProductLine) %>%
  summarise(sales = sum(values_adj)) %>%
  group_by(ProductLine) %>%
  mutate(inc = 100 * c(1,  tail(sales, -1)/head(sales, -1))) %>%
  arrange(ProductLine) %>%
  print(n = 110)

# COMMAND ----------

sales_input %>%
  print(n = 60)

# COMMAND ----------

tmp <- adj_forecasts %>%
  left_join(pl_forecast_sm)

# COMMAND ----------

tmp %>% glimpse()

# COMMAND ----------

adj_forecasts <- adj_forecasts %>%
  left_join(pl_forecast_sm) %>%
  group_by(Date, ProductLine) %>%
  mutate(values_pl_sm_adj = values_adj * (values / sum(values))) %>%
  ungroup() %>%
  group_by(Date, ProductLine, SalesMarket) %>%
  summarise(values_adj = sum(values_pl_sm_adj)) %>%
  ungroup()

# COMMAND ----------

adj_forecasts %>%
  left_join(pl_forecast_sm) %>%
  group_by(Date) %>%
  summarise(pl = sum(values),
            pl_adj = sum(values_adj)) %>%
  ggplot()+
  geom_line(aes(x = Date, y = pl, color = "pl"))+
  geom_line(aes(x = Date, y = pl_adj, color = "adj"))

# COMMAND ----------

adj_forecasts %>%
  left_join(pl_forecast_sm) %>%
  group_by(Date) %>%
  summarise(pl = sum(values),
            pl_adj = sum(values_adj)) %>%
  print(n = 144)

# COMMAND ----------

forecast_spark <- sdf_copy_to(sc, adj_forecasts %>%
                                    left_join(pl_forecast_sm),
                                  overwrite = TRUE)
path_out <- paste0("/mnt/blob/forecast/invoice/productline/current/pl_sm_wd_adj.csv")
spark_write_csv(forecast_spark, 
                path = path_out, 
                delimiter = ";", 
                mode = "overwrite")
path_out <- paste0("/mnt/blob/forecast/invoice/productline/archive/", Sys.Date() %>% str_replace_all("-", "_") ,"/pl_sm_wd_adj.csv")
spark_write_csv(forecast_spark, 
                path = path_out, 
                delimiter = ";", 
                mode = "overwrite")


