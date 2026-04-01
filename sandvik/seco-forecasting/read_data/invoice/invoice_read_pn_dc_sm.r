# Databricks notebook source
# MAGIC %run /Repos/sandvik/seco-forecasting/read_data/mount_datalake

# COMMAND ----------

user <- dbutils.secrets.get(scope = "azure_secrets", key = "sqluser")
password <- dbutils.secrets.get(scope = "azure_secrets", key = "sqlpassword")

url <- "jdbc:sqlserver://seco-azure-info-prod.database.windows.net:1433;database=seco-azure-info-prod"
SupplyingWarehouse <- spark_read_jdbc(sc,
                        name = "SupplyingWarehouse",
                        options = list(user = user,
                        password = password,
                        url = url,
                        dbtable = "pub.view_SupplyingWarehouse"))

# COMMAND ----------

path_in <- paste0("/mnt/blob/data/co_orderinvoice/", co_order_latest) 
income_pn_dc_sm <- spark_read_csv(sc, 
                       path = path_in,
                       delimiter = ";",
                       columns = list(
                         CompanyId = "character",
                         InvoiceYearMonth = "integer",
                         SupplyingWarehouse = "character",
                         ProductNumber = "character",
                         InvoicedQuantity = "double")) %>%
            rename(SalesMarket = CompanyId,
                  Quantity = InvoicedQuantity) %>%
            mutate(DateMonth = InvoiceYearMonth %>% paste0("01"),
                   SalesMarket = if_else(SalesMarket == "CA", "US", SalesMarket),
                   SupplyingWarehouse = if_else(SupplyingWarehouse == "EDC", "DCE", SupplyingWarehouse)) %>%
            mutate(SalesMarket = if_else(is.na(SalesMarket), "MISSING", SalesMarket)) %>%
            mutate(SupplyingWarehouse = if_else(is.na(SupplyingWarehouse), "MISSING", SupplyingWarehouse))            

# COMMAND ----------

income_pn_dc_sm <- income_pn_dc_sm %>%
  group_by(DateMonth, ProductNumber, SupplyingWarehouse, SalesMarket) %>%
  summarise(TotalOrders = sum(Quantity)) %>%
  ungroup() %>%
  collect() %>%
  mutate(DateMonth = DateMonth %>% as.Date(format = "%Y%m%d")) %>%
  filter(DateMonth <= end_date, !is.na(DateMonth)) %>%
  mutate(SalesMarket = if_else(SalesMarket %>% is.na(), "MISSING", SalesMarket)) %>%
  arrange(SupplyingWarehouse, ProductNumber, SalesMarket, DateMonth) 

# COMMAND ----------

income_pn_dc_sm %>%
  group_by(DateMonth, SupplyingWarehouse) %>%
  summarise(y = sum(TotalOrders)) %>%
  ggplot() +
  geom_line(aes(x = DateMonth, y = y, color = SupplyingWarehouse))

# COMMAND ----------

sc %>% spark_session() %>% invoke("catalog") %>% 
  invoke("clearCache")

# COMMAND ----------

income_pn_dc_sm %>% 
  group_by(DateMonth) %>%
  summarise(yraw = sum(TotalOrders)) %>%
  print(n = 200)

# COMMAND ----------

replace_outliers <- function(tbl){
  library(tidyverse)
  
  values <- tbl %>% pull(TotalOrders) 
  box <- boxplot(values, plot = FALSE, range = 4.5)
  outliers <- box$out
  values_no_outlier_mean <- values[!(values %in% outliers)] %>% mean()
  
  tbl %>%
    mutate(TotalOrders_no_outliers = if_else( TotalOrders %in% outliers, values_no_outlier_mean, TotalOrders))
   
}


# COMMAND ----------

library(parallel)
library(data.table)
cl <- makeCluster(15)

income_pn_dc_sm <- income_pn_dc_sm %>%
  group_split(SupplyingWarehouse, SalesMarket, ProductNumber) %>%
  parLapply(cl, .,replace_outliers) %>%
  rbindlist() %>%
  as_tibble()

stopCluster(cl)

# COMMAND ----------

income_pn_dc_sm %>%
  group_by(DateMonth, SupplyingWarehouse) %>%
  summarise(y = sum(TotalOrders),
            y_no_ouliers = sum(TotalOrders_no_outliers)) %>%
  ggplot() +
  geom_line(aes(x = DateMonth, y = y, color = SupplyingWarehouse))+
  geom_line(aes(x = DateMonth, y = y_no_ouliers, color = SupplyingWarehouse), , linetype = "dashed")

# COMMAND ----------

income_pn_dc_sm %>%
  group_by(DateMonth) %>%
  summarise(y = sum(TotalOrders),
            y_no_ouliers = sum(TotalOrders_no_outliers)) %>%
  ggplot() +
  geom_line(aes(x = DateMonth, y = y))+
  geom_line(aes(x = DateMonth, y = y_no_ouliers), , linetype = "dashed")
