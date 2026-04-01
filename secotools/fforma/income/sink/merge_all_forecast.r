# Databricks notebook source
# MAGIC %run /Repos/alejandro.kuratomi_hernandez@secotools.com/prophet_R_secotools/secotools/pipelines/mount_datalake

# COMMAND ----------

path_in <- paste0("/mnt/blob/data/iw_dimvproduct/", iw_dimvproduct_latest) 
product_info <- spark_read_csv(sc, 
                       path = path_in,
                       delimiter = ";"
                                   ) %>% 
                  select(ProductNumber, 
                         ProductLine,
                         ProductGroupDescription,
                         ProductAccountGroupDescription,
                         InventoryClassCode,
                         SupplyMethod) %>%
                  mutate(ProductLine = if_else(ProductLine %>% is.na(), "MISSING", ProductLine))

# COMMAND ----------

path_in <- paste0("/mnt/blob/data/dc_orderincome/", dc_order_latest) 
income_pn_dc_sm <- spark_read_csv(sc, 
                       path = path_in,
                       delimiter = ";",
                       columns = list(
                         SalesMarket = "character",
                         Warehouse = "character",
                         ItemNumber = "character",
                         TransactionDate = "integer",
                         OrderDate = "integer",
                         Quantity = "double")) %>%
            rename(ProductNumber = ItemNumber,
                   SupplyingWarehouse = Warehouse,
                   TotalOrders = Quantity) %>%
            mutate(Date = paste0(TransactionDate %>% substr(1, 4), "-", TransactionDate %>% substr(5, 6), "-01"),
                   SalesMarket = if_else(SalesMarket == "CA", "US", SalesMarket),
                   SupplyingWarehouse = if_else(SupplyingWarehouse == "EDC", "DCE", SupplyingWarehouse)) %>%
            mutate(SalesMarket = if_else(SalesMarket %>% is.na(), "MISSING", SalesMarket),
                   dm = Date %>% to_date("yyyy-MM-dd")) %>%
            filter(dm <= end_date) %>%
            group_by(Date, SupplyingWarehouse, SalesMarket, ProductNumber) %>%
            summarise(TotalOrders = sum(TotalOrders)) %>%
            ungroup() %>%
            left_join(product_info)

# COMMAND ----------

path_in <- paste0("/mnt/blob/forecast/income/productnumber_dc_sm/current/pn_dc_sm_adj.csv")
pn_dc_sm_forecast <- spark_read_csv(sc, 
                       path = path_in,
                       delimiter = ";",
                       columns = list(
                         Date = "character",
                         ProductNumber = "character",
                         SupplyingWarehouse = "character",
                         SalesMarket = "character",
                         values = "double")
                                   ) %>%
  filter(Date != "2022-01-01")

# COMMAND ----------

pn_dc_sm_forecast %>%
  group_by(Date) %>%
  summarise(y = sum(values)) %>%
  collect() %>%
  mutate(Date = Date %>% as.Date()) %>%
  ggplot()+
  geom_line(aes(x = Date, y = y))

# COMMAND ----------

path_in <- paste0("/mnt/blob/forecast/income/productline/current/pl_adj_wd.csv")
pl_forecast <- spark_read_csv(sc, 
                       path = path_in,
                       delimiter = ";") %>%
               filter(source != "x") %>%
               select(-source) %>%
               rename(values_pl = values,
                      values_pl_adj = values_adj)
                                   

# COMMAND ----------

adj_forecast <- pn_dc_sm_forecast %>%
  left_join(product_info) %>%
  left_join(pl_forecast)

# COMMAND ----------

adj_forecast <- adj_forecast %>%
  group_by(Date, ProductLine) %>%
  mutate(p = values / sum(values)) %>%
  ungroup() %>%
  mutate(values = p * values_pl,
         values_adj = p * values_pl_adj) %>%
  select(-c(values_pl, values_pl_adj, p))

# COMMAND ----------

adj_forecast %>%
  group_by(Date) %>%
  summarise(v = sum(values),
            vadj = sum(values_adj)) %>%
  collect() %>%
  mutate(Date = Date %>% as.Date()) %>%
  ggplot()+
  geom_line(aes(x = Date, y = v, color = "values"))+
  geom_line(aes(x = Date, y = vadj, color = "adjusted"))

# COMMAND ----------

all_data <- income_pn_dc_sm %>%
  full_join(adj_forecast) %>%
  arrange(ProductNumber, Date) %>%
  sdf_register("all_data") 

tbl_cache(sc, "all_data")

# COMMAND ----------

all_data %>%
  group_by(Date) %>%
  summarise(to = sum(TotalOrders),
            vadj = sum(values_adj)) %>%
  collect() %>% 
  arrange(Date) %>%
  print(n = 300)

# COMMAND ----------

all_data %>%
  group_by(Date) %>%
  summarise(to = sum(TotalOrders),
            vadj = sum(values_adj)) %>%
  collect() %>%
  mutate(Date = Date %>% as.Date()) %>%
  ggplot()+
  geom_line(aes(x = Date, y = to, color = "to"))+
  geom_line(aes(x = Date, y = vadj, color = "vadj"))

# COMMAND ----------

date_today <- Sys.Date() %>% str_replace_all(pattern = "-", replacement = "_")

path_out_current <- paste0("/mnt/blob/forecast/income/final/", "current", "/income_forecast_wd.csv")
path_out_archive <- paste0("/mnt/blob/forecast/income/final/archive/", date_today, "/income_forecast_wd.csv")

dbutils.fs.rm(path_out_current, TRUE)
dbutils.fs.rm(path_out_archive, TRUE)

spark_write_csv(all_data %>% sdf_repartition(1), 
                path = path_out_current, 
                delimiter = ";", 
                mode = "overwrite")

spark_write_csv(all_data %>% sdf_repartition(1), 
                path = path_out_archive, 
                delimiter = ";", 
                mode = "overwrite")


