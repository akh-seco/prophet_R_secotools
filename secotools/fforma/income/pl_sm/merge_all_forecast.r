# Databricks notebook source
# MAGIC %run /Repos/alejandro.kuratomi_hernandez@secotools.com/prophet_R_secotools/secotools/pipelines/mount_datalake

# COMMAND ----------

user <- dbutils.secrets.get(scope = "azure_secrets", key = "sqluser")
password <- dbutils.secrets.get(scope = "azure_secrets", key = "sqlpassword")

url <- "jdbc:sqlserver://seco-azure-info-prod.database.windows.net:1433;database=seco-azure-info-prod"
product_info <- spark_read_jdbc(sc,
                        name = "product_info",
                        options = list(user = user,
                        password = password,
                        url = url,
                        dbtable = "pub.view_ProductBasic")) %>%
  rename(ProductNumber = ItemNumber) %>%
  select(ProductNumber, ProductLine) %>%
  mutate(ProductLine = if_else(ProductLine %>% is.na(), "MISSING", ProductLine))

# COMMAND ----------

path_in <- paste0("/mnt/blob/forecast/income/productline/current/pl_sm_wd_adj.csv")
pl_forecast <- spark_read_csv(sc, 
                       path = path_in,
                       delimiter = ";") %>%
               filter(source != "x") %>%
               select(-source) %>%
               rename(values_pl = values,
                      values_pl_adj = values_adj)
                                   

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
            mutate(SalesMarket = if_else(SalesMarket == "IP", "IN", SalesMarket)) %>%
            mutate(SalesMarket = if_else(SalesMarket %>% is.na(), "MISSING", SalesMarket),
                   dm = Date %>% to_date("yyyy-MM-dd")) %>%
            filter(dm <= end_date) %>%
            group_by(Date, SupplyingWarehouse, SalesMarket, ProductNumber) %>%
            summarise(TotalOrders = sum(TotalOrders)) %>%
            ungroup() %>%
            left_join(product_info)        

# COMMAND ----------

last_date <- pl_forecast %>% pull(Date) %>% max()
path_in <- paste0("/mnt/blob/forecast/income/productnumber_dc_sm/current/pn_dc_sm.csv")
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
  filter(Date != "2022-01-01", Date <= last_date) %>%
  mutate(SalesMarket = if_else(SalesMarket %>% is.na(), "MISSING", SalesMarket)) %>%
  mutate(SalesMarket = if_else(SalesMarket == "IP", "IN", SalesMarket)) %>% 
  group_by(Date, ProductNumber, SupplyingWarehouse, SalesMarket) %>%
  summarise(values = sum(values)) %>%
  ungroup()

# COMMAND ----------

pn_dc_sm_forecast %>%
  group_by(Date) %>%
  summarise(y = sum(values)) %>%
  collect() %>%
  mutate(Date = Date %>% as.Date()) %>%
  ggplot()+
  geom_line(aes(x = Date, y = y))

# COMMAND ----------

pl_forecast %>%
  group_by(Date) %>%
  summarise(y = sum(values_pl),
            y_adj = sum(values_pl_adj)) %>%
  collect() %>%
  arrange(Date) %>%
  print(n = 200)

# COMMAND ----------

adj_forecast <- pn_dc_sm_forecast %>%
  left_join(product_info) %>%
  mutate(ProductLine = if_else(ProductLine %>% is.na(), "MISSING", ProductLine)) %>%
  left_join(pl_forecast)

# COMMAND ----------

adj_forecast %>% glimpse()

# COMMAND ----------

adj_forecast <- adj_forecast %>%
  group_by(Date, ProductLine, SalesMarket) %>%
  mutate(p = values / sum(values),
         n = n()) %>%
  mutate(p = if_else(is.na(p), 1/n, p)) %>%
  ungroup() %>%
  mutate(values = p * values_pl,
         values_adj = p * values_pl_adj) %>%
  select(-c(values_pl, values_pl_adj, p, n))

# COMMAND ----------

adj_forecast %>%
  #filter(ProductLine == "Stationary") %>%
  group_by(Date) %>%
  summarise(y = sum(values),
            y_adj = sum(values_adj)) %>%
  arrange(Date) %>%
  print(n = 200)

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

path_out_current <- paste0("/mnt/blob/forecast/income/final/", "current", "/income_forecast.csv")
path_out_archive <- paste0("/mnt/blob/forecast/income/final/archive/", date_today, "/income_forecast.csv")

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

# COMMAND ----------

pl_forecast %>%
  group_by(Date) %>%
  summarise(
            values = sum(values_pl),
            values_adj = sum(values_pl_adj)) %>%
  arrange(Date) %>%
  print(n = 100)

# COMMAND ----------

all_data %>%
  group_by(Date) %>%
  summarise(TotalOrder = sum(TotalOrders),
            values = sum(values),
            values_adj = sum(values_adj)) %>%
  arrange(Date) %>%
  print(n = 100)


