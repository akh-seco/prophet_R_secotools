# Databricks notebook source
# MAGIC %run /Repos/secotools/pipelines/mount_datalake

# COMMAND ----------

path_in <- paste0("/mnt/blob/forecast/invoice/productnumber_dc/", "current", "/pn_dc.csv")
pn_dc <- spark_read_csv(sc, 
                       path = path_in,
                       delimiter = ";",
                       columns = list(
                         Date = "character",
                         ProductNumber = "character",
                         SupplyingWarehouse = "character",
                         values = "double",
                         source = "character")
                                   ) %>%
  filter(source != "x") %>%
  rename(values_pn_dc = values) %>%
  mutate(SupplyingWarehouse = if_else(is.na(SupplyingWarehouse), "MISSING", SupplyingWarehouse))

# COMMAND ----------

pn_dc %>%
  group_by(Date) %>%
  summarise(y = sum(values_pn_dc)) %>%
  collect() %>%
  mutate(Date = Date %>% as.Date()) %>%
  ggplot()+
  geom_line(aes(x = Date, y = y))

# COMMAND ----------

path_in <- paste0("/mnt/blob/forecast/invoice/productnumber_dc_sm/", "current", "/pn_dc_sm.csv")
pn_dc_sm <- spark_read_csv(sc, 
                       path = path_in,
                       delimiter = ";",
                       columns = list(
                         Date = "character",
                         ProductNumber = "character",
                         SupplyingWarehouse = "character",
                         SalesMarket = "character",
                         values = "double",
                         source = "character")
                                   ) %>%
  rename(values_pn_dc_sm = values) %>%
  mutate(SupplyingWarehouse = if_else(is.na(SupplyingWarehouse), "MISSING", SupplyingWarehouse))

# COMMAND ----------

pn_dc_sm %>%
  group_by(SupplyingWarehouse) %>%
  tally()

# COMMAND ----------

pn_dc %>%
  group_by(SupplyingWarehouse) %>%
  tally()

# COMMAND ----------

adj_forecasts <- pn_dc_sm %>%
  left_join(pn_dc) %>%
  group_by(Date, SupplyingWarehouse, ProductNumber) %>%
  mutate(y = sum(values_pn_dc_sm)) %>%
  mutate(y = if_else(y == 0, 0.01, y)) %>%
  mutate(p = values_pn_dc_sm / y) %>%
  ungroup() %>%
  mutate(values = p * values_pn_dc)

# COMMAND ----------

adj_forecasts %>%
  filter(is.na(values)) %>%
  sdf_nrow

# COMMAND ----------

adj_forecasts <- pn_dc_sm %>%
  left_join(pn_dc) %>%
  group_by(Date, SupplyingWarehouse, ProductNumber) %>%
  mutate(y = sum(values_pn_dc_sm)) %>%
  mutate(y = if_else(y == 0, 0.01, y)) %>%
  mutate(p = values_pn_dc_sm / y) %>%
  ungroup() %>%
  mutate(values = p * values_pn_dc)

# COMMAND ----------

adj_forecasts %>%
  filter(!is.na(values)) %>%
  arrange(-values) %>%
  select(-c(source, SupplyingWarehouse)) %>%
  print(n = 200) 

# COMMAND ----------

adj_forecasts %>%
  group_by(Date) %>%
  summarise(raw = sum(values_pn_dc_sm),
            adj = sum(values)) %>%
  collect() %>%
  mutate(Date = Date %>% as.Date()) %>%
  filter(Date <= "2026-12-01") %>%
  ggplot()+
  geom_line(aes(x = Date, y = raw, color = "raw"))+
  geom_line(aes(x = Date, y = adj, color = "adj"))

# COMMAND ----------

adj_forecasts <- adj_forecasts %>%
  select(-c(values_pn_dc_sm, source, values_pn_dc, y, p))

# COMMAND ----------

path_out_current <- paste0("/mnt/blob/forecast/invoice/productnumber_dc_sm/current/pn_dc_sm_adj.csv")
path_out_archive <- paste0("/mnt/blob/forecast/invoice/productnumber_dc_sm/archive/", Sys.Date() %>% str_replace_all("-", "_") ,"/pn_dc_sm_adj.csv")

dbutils.fs.rm(path_out_current, TRUE)
dbutils.fs.rm(path_out_archive, TRUE)

spark_write_csv(adj_forecasts, 
                path = path_out_current, 
                delimiter = ";", 
                mode = "overwrite")
spark_write_csv(adj_forecasts, 
                path = path_out_archive, 
                delimiter = ";", 
                mode = "overwrite")


