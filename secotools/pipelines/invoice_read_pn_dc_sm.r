# Databricks notebook source
# MAGIC %run /Repos/alejandro.kuratomi_hernandez@secotools.com/prophet_R_secotools/pipelines/mount_datalake

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
                  Quantity = InvoicedQuantity) %>%
            mutate(DateMonth = InvoiceYearMonth %>% paste0("01"),
                   SalesMarket = if_else(SalesMarket == "CA", "US", SalesMarket),
                   SupplyingWarehouse = if_else(SupplyingWarehouse == "EDC", "DCE", SupplyingWarehouse)) %>%
            mutate(SalesMarket = if_else(is.na(SalesMarket), "MISSING", SalesMarket)) %>%
            mutate(SupplyingWarehouse = if_else(is.na(SupplyingWarehouse), "MISSING", SupplyingWarehouse))            

# COMMAND ----------

invoice_pn_dc_sm <- invoice_pn_dc_sm %>%
  group_by(DateMonth, ProductNumber, SupplyingWarehouse, SalesMarket) %>%
  summarise(TotalOrders = sum(Quantity)) %>%
  ungroup() %>%
  collect() %>%
  mutate(DateMonth = DateMonth %>% as.Date(format = "%Y%m%d")) %>%
  filter(DateMonth <= end_date, !is.na(DateMonth)) %>%
  filter( !(SupplyingWarehouse == "MISSING" & DateMonth <= "2016-01-01") ) %>%
  mutate(SalesMarket = if_else(SalesMarket %>% is.na(), "MISSING", SalesMarket)) %>%
  arrange(SupplyingWarehouse, ProductNumber, SalesMarket, DateMonth) 

# COMMAND ----------



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
library(parallel)
library(data.table)
cl <- makeCluster(15)

invoice_pn_dc_sm <- invoice_pn_dc_sm %>%
  group_split(SupplyingWarehouse, SalesMarket, ProductNumber) %>%
  parLapply(cl, .,replace_outliers) %>%
  rbindlist() %>%
  as_tibble()

stopCluster(cl)

# COMMAND ----------

invoice_pn_dc_sm %>%
  group_by(DateMonth, SupplyingWarehouse) %>%
  summarise(y = sum(TotalOrders),
            y_no_ouliers = sum(TotalOrders_no_outliers)) %>%
  ggplot() +
  geom_line(aes(x = DateMonth, y = y, color = SupplyingWarehouse))+
  geom_line(aes(x = DateMonth, y = y_no_ouliers, color = SupplyingWarehouse), , linetype = "dashed")

# COMMAND ----------

sc %>% spark_session() %>% invoke("catalog") %>% 
  invoke("clearCache")

# COMMAND ----------

invoice_pn_dc_sm %>% 
  group_by(DateMonth) %>%
  summarise(yraw = sum(TotalOrders)) %>%
  print(n = 200)

# COMMAND ----------

invoice_pn_dc_sm %>%
  group_by(ProductNumber, SalesMarket, SupplyingWarehouse) 
