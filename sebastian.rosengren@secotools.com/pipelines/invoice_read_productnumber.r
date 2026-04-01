# Databricks notebook source
# MAGIC %run /Repos/sebastian.rosengren@secotools.com/pipelines/mount_datalake

# COMMAND ----------

path_in <- paste0("/mnt/blob/data/co_orderinvoice/", co_order_latest) 
invoice_pn <- spark_read_csv(sc, 
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

invoice_pn <- invoice_pn %>%
  group_by(DateMonth, ProductNumber, SupplyingWarehouse) %>%
  summarise(TotalOrders = sum(Quantity)) %>%
  ungroup() %>%
  collect() %>%
  mutate(DateMonth = DateMonth %>% as.Date(format = "%Y%m%d")) %>%
  filter(DateMonth <= end_date,
         !is.na(ProductNumber)) %>%
  arrange(SupplyingWarehouse, ProductNumber, DateMonth)

# COMMAND ----------

invoice_pn %>%
  group_by(DateMonth, SupplyingWarehouse) %>%
  summarise(y = sum(TotalOrders)) %>%
  ggplot() +
  geom_line(aes(x = DateMonth, y = y, color = SupplyingWarehouse))

# COMMAND ----------

sc %>% spark_session() %>% invoke("catalog") %>% 
  invoke("clearCache")

# COMMAND ----------

replace_outliers <- function(tbl){
  
  values <- tbl %>% pull(TotalOrders) 
  box <- boxplot(values, plot = FALSE, range = 4.5)
  outliers <- box$out
  values_no_outlier_mean <- values[!(values %in% outliers)] %>% mean()
  
  tbl %>%
    mutate(TotalOrders_no_outliers = if_else( TotalOrders %in% outliers, values_no_outlier_mean, TotalOrders))
   
}

# COMMAND ----------

invoice_pn <- invoice_pn %>%
  group_by(SupplyingWarehouse, ProductNumber) %>%
  do(replace_outliers(.)) %>%
  ungroup()

# COMMAND ----------

invoice_pn %>%
  group_by(DateMonth, SupplyingWarehouse) %>%
  summarise(y = sum(TotalOrders),
            yno = sum(TotalOrders_no_outliers)) %>%
  ggplot() +
  geom_line(aes(x = DateMonth, y = y, color = SupplyingWarehouse))+
  geom_line(aes(x = DateMonth, y = yno, color = SupplyingWarehouse), linetype = "dashed")