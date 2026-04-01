# Databricks notebook source
# MAGIC %run /Repos/secotools/pipelines/mount_datalake

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

sales_input <- sales_input %>%
  left_join(pl_forecast_sm %>%
  group_by(SalesMarket, Sales_Region) %>%
  tally() %>%
  select(-n))

# COMMAND ----------

sales_input

# COMMAND ----------

pl_forecast_sm <- pl_forecast_sm %>%
  mutate(month = month(Date), year = year(Date)) %>%
  filter(year <= (sales_input %>% pull(year) %>% max())) %>%
  mutate(history = Date <= end_date)

# COMMAND ----------

pl_forecast_sm %>% glimpse()

# COMMAND ----------

sales_input

# COMMAND ----------

last_full_year <- pl_forecast_sm %>%
  filter(source == "x") %>%
  select(year) %>%
  max()-1

apply_sales_adj <- function(tbl) {
  
  
  this_year <- tbl %>% pull(year) %>% unique()
  sm <- tbl %>% pull(SalesMarket) %>% unique()
  pl <- tbl %>% pull(ProductLine) %>% unique()
  
  if ((this_year <= last_full_year) | pl == "MISSING"){
    last_year_sales <<- tbl %>% pull(values) %>% sum()
    
    tbl %>%
      mutate(values_adj = values)
    
  } 
  
  else {
    
    sales_increase <- sales_input %>%
      filter(SalesMarket == sm, 
             year == this_year, 
             ProductLine == pl) %>%
      pull(increase)
    
    has_history <- tbl %>%
      filter(history) %>%
      pull(values) %>%
      sum(na.rm = TRUE)
    
    forecast <- tbl %>%
      filter(!history) %>%
      pull(values) %>%
      sum(na.rm = TRUE)
    
    increase <- ( sales_increase * last_year_sales - has_history ) / forecast
    if ( is.infinite(increase) | is.na(increase) ) {
     
      #print(sm)
      months_left <- tbl %>%
        filter(history) %>%
        pull(values) %>%
        length() - 12
      
      #print(months_left)
      
      increase <- (sales_increase * last_year_sales - has_history) / abs(months_left)
      #print(increase)
      tbl <- tbl %>%
        mutate(values_adj = if_else(history, values, increase))
    } else {
      tbl <- tbl %>%
      mutate(values_adj = if_else(history, values, increase * values)) 
    }
    
    #tbl <- tbl %>%
    #  mutate(values_adj = if_else(history, values, increase * values)) %>%
    #  mutate(values_adj = if_else(is.na(values_adj), increase, values_adj))
    
    last_year_sales <<- tbl %>% pull(values_adj) %>% sum()
    
    tbl
    
  }
  
  
  
}

# COMMAND ----------

adj_forecasts <- pl_forecast_sm %>%
  group_by(ProductLine, SalesMarket, year) %>%
  do(apply_sales_adj(.)) %>%
  ungroup()

# COMMAND ----------

adj_forecasts %>%
  group_by(Sales_Region, ProductLine, year) %>%
  summarise(sales = sum(values_adj)) %>%
  group_by(ProductLine, Sales_Region) %>%
  mutate(inc = 100 * c(1,  tail(sales, -1)/head(sales, -1))) %>%
  arrange(ProductLine, Sales_Region) %>%
  print(n = 600)

# COMMAND ----------

adj_forecasts %>%
  group_by(Date) %>%
  summarise(values = sum(values),
            values_adj = sum(values_adj, na.rm = T)) %>%
  print(n = 200)

# COMMAND ----------

adj_forecasts <- adj_forecasts %>%
  left_join(pl_forecast_sm) %>%
  select(Date, ProductLine, SalesMarket, values_adj, values, source)

# COMMAND ----------

adj_forecasts %>%
  mutate(year = year(Date)) %>%
  group_by(year) %>%
  summarise(y = sum(values),
            y_adj = sum(values_adj, na.rm = TRUE))

# COMMAND ----------

sales_input %>%
  filter(ProductLine == "MISSING") %>%
  print(n = 400)

# COMMAND ----------

adj_forecasts %>%
  group_by(Date) %>%
  summarise(v = sum(values),
            vadj = sum(values_adj)) %>%
  print(n = 400)

# COMMAND ----------

adj_forecasts %>%
  #filter(SalesMarket == "SE") %>%
  filter(ProductLine == "MISSING") %>%
  group_by(Date) %>%
  summarise(v = sum(values),
            vadj = sum(values_adj)) %>%
  collect() %>%
  mutate(Date = Date %>% as.Date()) %>%
  ggplot()+
  geom_line(aes(x = Date, y = v, color = "values"))+
  geom_line(aes(x = Date, y = vadj, color = "adjusted"))

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


