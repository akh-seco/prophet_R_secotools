# Databricks notebook source
# MAGIC %run /Repos/alejandro.kuratomi_hernandez@secotools.com/prophet_R_secotools/secotools/pipelines/mount_datalake

# COMMAND ----------

path_in <- paste0("/mnt/blob/forecast/income/productline/custom/pl_sm_wd.csv")
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
  group_by(Date, ProductLine) %>%
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
                      mutate(increase = c(1, 1, 1, 1, 1, 1,
                                         1, 1.1425, 1.0825, 1.0663, 1.0505, 1.0505,
                                         1, 1.0838, 1.0485, 1.0390, 1.0297, 1.0297,
                                         1, 1, 1, 1, 1, 1,
                                         1, 1.2345, 1.2394, 1.2160, 1.47, 1.47,
                                         1, 1.2345, 1.2394, 1.2160, 1.47, 1.47,
                                         1, 1.1676, 1.097, 1.078, 1.0594, 1.0594,
                                         1, 1.0754, 1.0437, 1.0351, 1.0267, 1.0267,
                                         1, 1.0552, 1.0591, 1.0644, 1.0625, 1.0625))

# COMMAND ----------

pl_forecast <- pl_forecast %>%
  mutate(month = month(Date), year = year(Date)) %>%
  filter(year < 2027)

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
path_out <- paste0("/mnt/blob/forecast/income/productline/custom/pl_sm_wd_adj.csv")
spark_write_csv(forecast_spark, 
                path = path_out, 
                delimiter = ";", 
                mode = "overwrite")


