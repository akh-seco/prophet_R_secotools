# Databricks notebook source
# MAGIC %run /Repos/sebastian.rosengren@secotools.com/pipelines/mount_datalake

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

path_in <- paste0("/mnt/blob/data/co_orderinvoice/", co_order_latest) 
invoice_pn_dc_sm <- spark_read_csv(sc, 
                       path = path_in,
                       delimiter = ";",
                       columns = list(
                         CompanyId = "character",
                         InvoiceYearMonth = "integer",
                         SupplyingWarehouse = "character",
                         ProductNumber = "character",
                         InvoicedQuantity = "double")) %>%
            rename(SalesMarket = CompanyId,
                   TotalOrders = InvoicedQuantity) %>%
            mutate(Date = paste0(InvoiceYearMonth %>% substr(1, 4), "-", InvoiceYearMonth %>% substr(5, 6), "-01"),
                   SalesMarket = if_else(SalesMarket == "CA", "US", SalesMarket),
                   SupplyingWarehouse = if_else(SupplyingWarehouse == "EDC", "DCE", SupplyingWarehouse)) %>%
            mutate(SalesMarket = if_else(SalesMarket %>% is.na(), "MISSING", SalesMarket),
                   dm = Date %>% to_date("yyyy-MM-dd")) %>% 
            mutate(SupplyingWarehouse = if_else(SupplyingWarehouse %>% is.na(), "MISSING", SupplyingWarehouse)) %>%
            filter(dm <= end_date) %>%
            group_by(Date, SupplyingWarehouse, SalesMarket, ProductNumber) %>%
            summarise(TotalOrders = sum(TotalOrders)) %>%
            ungroup() %>%
            left_join(product_info) %>%
            mutate(ProductLine = if_else(ProductLine %>% is.na(), "MISSING", ProductLine))

# COMMAND ----------

invoice_pn_dc_sm %>% glimpse()

# COMMAND ----------

path_in <- paste0("/mnt/blob/forecast/invoice/productnumber_dc_sm/current/pn_dc_sm_adj.csv")
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
  filter(Date != "2022-01-01", Date <= "2025-12-01") %>%
  mutate(SalesMarket = if_else(SalesMarket %>% is.na(), "MISSING", SalesMarket))

# COMMAND ----------

pn_dc_sm_forecast %>%
  group_by(Date) %>%
  summarise(y = sum(values)) %>%
  collect() %>%
  mutate(Date = Date %>% as.Date()) %>%
  ggplot()+
  geom_line(aes(x = Date, y = y))

# COMMAND ----------

path_in <- paste0("/mnt/blob/forecast/invoice/productline/current/pl_sm_wd_adj.csv")
pl_forecast <- spark_read_csv(sc, 
                       path = path_in,
                       delimiter = ";") %>%
               filter(source != "x") %>%
               select(-source) %>%
               rename(values_pl = values,
                      values_pl_adj = values_adj) %>%
  mutate(SalesMarket = if_else(SalesMarket %>% is.na(), "MISSING", SalesMarket))                                   

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

adj_forecast %>%
  #filter(SalesMarket == "SE") %>%
  group_by(Date) %>%
  summarise(v = sum(values),
            vadj = sum(values_adj)) %>%
  collect() %>%
  mutate(Date = Date %>% as.Date()) %>%
  ggplot()+
  geom_line(aes(x = Date, y = v, color = "values"))+
  geom_line(aes(x = Date, y = vadj, color = "adjusted"))

# COMMAND ----------

adj_forecast %>%
  #filter(SalesMarket == "SE") %>%
  group_by(Date, SupplyingWarehouse) %>%
  summarise(v = sum(values),
            vadj = sum(values_adj)) %>%
  collect() %>%
  mutate(Date = Date %>% as.Date()) %>%
  ggplot()+
  geom_line(aes(x = Date, y = v, color = SupplyingWarehouse))+
  geom_line(aes(x = Date, y = vadj, color = SupplyingWarehouse), linetype = "dashed")

# COMMAND ----------

all_data <- invoice_pn_dc_sm %>%
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
  group_by(Date) %>%
  mutate(values = sum(mean(c(to, vadj), na.rm = TRUE))) %>%
  group_by(year = year(Date)) %>%
  summarise(y = sum(values)) %>%
  mutate(inc = c(1, tail(y, -1) / head(y, -1)))
  

# COMMAND ----------

all_data %>%
  group_by(Date) %>%
  summarise(to = sum(TotalOrders),
            vadj = sum(values_adj),
            y = sum(values)) %>%
  collect() %>%
  mutate(Date = Date %>% as.Date()) %>%
  ggplot()+
  geom_line(aes(x = Date, y = to, color = "to"))+
  geom_line(aes(x = Date, y = vadj, color = "vadj"))+
  geom_line(aes(x = Date, y = y, color = "stat"))

# COMMAND ----------

tmp <- all_data %>%
  group_by(ProductNumber, ProductLine) %>%
  tally() %>%
  collect()

# COMMAND ----------

tmp %>%
  filter(ProductLine == "MISSING")

# COMMAND ----------

all_data_old_format <- all_data %>%
  filter(Date >= end_date) %>%
  select(Date, SupplyingWarehouse, SalesMarket, ProductNumber, values_adj) %>%
  rename(CompanyId = SalesMarket, 
         values = values_adj)

# COMMAND ----------

date_today <- Sys.Date() %>% str_replace_all(pattern = "-", replacement = "_")

path_out_current <- paste0("/mnt/blob/forecast/invoice/final/", "current", "/invoice_forecast.csv")
path_out_archive <- paste0("/mnt/blob/forecast/invoice/final/archive/", date_today, "/invoice_forecast.csv")

dbutils.fs.rm(path_out_current, TRUE)
dbutils.fs.rm(path_out_archive, TRUE)

#spark_write_csv(all_data %>% sdf_repartition(1), 
#                path = path_out_current, 
#                delimiter = ";", 
#                mode = "overwrite")

spark_write_csv(all_data %>% sdf_repartition(1), 
                path = path_out_archive, 
                delimiter = ";", 
                mode = "overwrite")
file_list <- dbutils.fs.ls(path_out_archive)

for(lst in file_list) {
  if(!grepl(".csv", lst$name, fixed = TRUE)) dbutils.fs.rm(lst$path, TRUE)
  else{
    dbutils.fs.cp(lst$path, paste0("/mnt/blob/forecast/invoice/final/", "current", "/invoice_forecast.csv"))
  }
}


