# Databricks notebook source
# MAGIC %run /Repos/sandvik/seco-forecasting/read_data/income/income_read_productline_sm

# COMMAND ----------

tmp <- income %>%
  rename(Date = DateMonth,
         values = TotalOrders_raw) %>%
  select(Date, 
         ProductLine,
         SalesMarket,
         values) %>%
  mutate(source = "x")

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

pl_forecast_sm_x <- tmp %>%
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

pl_forecast_sm %>%
  group_by(SalesMarket) %>%
  summarise(sr = unique(Sales_Region)) %>%
  print(n = 100)

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
         increase = (increase + 100) / 100) #%>%
  #filter(ProductLine != "Holemaking")

# COMMAND ----------

sales_input <- sales_input %>%
  left_join(pl_forecast_sm %>%
  group_by(SalesMarket, Sales_Region) %>%
  tally() %>%
  select(-n)) %>%
  mutate(ProductLine = if_else(ProductLine == "Stationary", "Turning", ProductLine)) %>%
  mutate(ProductLine = if_else(ProductLine == "Indexable Milling", "Insert Milling & Drilling", ProductLine)) %>%
  mutate(ProductLine = if_else(ProductLine == "Holemaking", "Solid Round Tools", ProductLine)) %>%
  mutate(ProductLine = if_else(ProductLine == "Solid Milling", "Solid Round Tools", ProductLine)) %>%
  group_by(Sales_Region, SalesMarket, ProductLine, year) %>%
  summarise(increase = max(increase)) %>%
  ungroup()

# COMMAND ----------

sales_input %>%
  print(n = 1000)

# COMMAND ----------

pl_forecast_sm <- pl_forecast_sm %>%
  mutate(month = month(Date), year = year(Date)) %>%
  mutate(history = Date <= end_date)

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
    print(c(this_year, pl, sm))
    if (length(increase) == 0) increase = Inf
    if ( is.infinite(increase) | is.na(increase)) {
     
      months_left <- tbl %>%
        filter(history) %>%
        pull(values) %>%
        length() - 12
      
      increase <- (sales_increase * last_year_sales - has_history) / abs(months_left)
      #print(c(this_year, pl, sm))
      #print(increase)

      tbl <- tbl %>%
        mutate(values_adj = if_else(history, values, increase))

    } else {
      tbl <- tbl %>%
      mutate(values_adj = if_else(history, values, increase * values)) 
    }

    last_year_sales <<- tbl %>% pull(values_adj) %>% sum()
    
    tbl
    
  }
}

# COMMAND ----------

adj_forecasts <- pl_forecast_sm %>%
  mutate(values_adj = values)

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

adj_forecasts %>%
  group_by(Date) %>%
  summarise(v = sum(values),
            vadj = sum(values_adj)) %>%
  print(n = 400)

# COMMAND ----------

adj_forecasts %>%
  group_by(Date) %>%
  summarise(v = sum(values),
            vadj = sum(values_adj)) %>%
  ggplot()+
  geom_line(aes(x = Date, y = v))+
  geom_line(aes(x = Date, y = vadj), linetype = "dashed")

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
