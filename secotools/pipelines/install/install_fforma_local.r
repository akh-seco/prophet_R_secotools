# Databricks notebook source
library(tidyverse)

# COMMAND ----------

install.packages("/dbfs/FileStore/tables/xgboost_1_5_0_1_tar.gz", repos = NULL)

# COMMAND ----------

install.packages("/dbfs/FileStore/tables/forecast_8_16_tar.gz", repos = NULL)

# COMMAND ----------

install.packages("/dbfs/FileStore/tables/Mcomp_2_8_tar.gz", repos = NULL)

# COMMAND ----------

install.packages("forecast")

# COMMAND ----------

library(prophet)

# COMMAND ----------

#Install on driver
install.packages("/dbfs/FileStore/tables/forecast_8_16_tar.gz", repos = NULL)
install.packages("/dbfs/FileStore/tables/Mcomp_2_8_tar.gz", repos = NULL)
install.packages("/dbfs/FileStore/tables/M4metalearning_0_0_0_9000_tar.gz", repos = NULL)
devtools::install_github("pmontman/customxgboost")
devtools::install_github("pmontman/tsfeatures")
install.packages("/dbfs/FileStore/tables/smooth_3_1_5_tar.gz", repos = NULL)

# COMMAND ----------

#Install on workers
library(sparklyr)
sc <- spark_connect(method = "databricks")

nr_workers <- spark_context(sc) %>% invoke("getExecutorMemoryStatus") %>% names() %>% length() - 1
print(nr_workers)

SparkR::spark.lapply(seq(1,nr_workers), function(x){
install.packages("/dbfs/FileStore/tables/forecast_8_16_tar.gz", repos = NULL)
install.packages("/dbfs/FileStore/tables/Mcomp_2_8_tar.gz", repos = NULL)
install.packages("/dbfs/FileStore/tables/M4metalearning_0_0_0_9000_tar.gz", repos = NULL)
devtools::install_github("pmontman/customxgboost")
devtools::install_github("pmontman/tsfeatures")
install.packages("/dbfs/FileStore/tables/smooth_3_1_5_tar.gz", repos = NULL)
}
                     ) 


