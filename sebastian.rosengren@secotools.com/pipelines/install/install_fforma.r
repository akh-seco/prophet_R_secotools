# Databricks notebook source
library(tidyverse)

# COMMAND ----------

#Install on driver
install.packages("forecast", repos = "https://mran.revolutionanalytics.com/snapshot/2021-10-25")
install.packages("Mcomp", repos = "https://mran.revolutionanalytics.com/snapshot/2021-10-25")
#install.packages("parallel", repos = "https://mran.revolutionanalytics.com/snapshot/2021-10-25")
install.packages("/dbfs/FileStore/tables/M4metalearning_0_0_0_9000_tar.gz", repos = NULL)
devtools::install_github("pmontman/customxgboost")
#devtools::install_github("pmontman/tsfeatures")
install.packages("tsfeatures")
install.packages("smooth")

# COMMAND ----------

#Install on workers
library(sparklyr)
sc <- spark_connect(method = "databricks")

nr_workers <- spark_context(sc) %>% invoke("getExecutorMemoryStatus") %>% names() %>% length() - 1
print(nr_workers)

SparkR::spark.lapply(seq(1,nr_workers), function(x){
  install.packages("smooth", repos = "https://mran.revolutionanalytics.com/snapshot/2021-10-25")
  install.packages("forecast", repos = "https://mran.revolutionanalytics.com/snapshot/2021-10-25")
  install.packages("Mcomp", repos = "https://mran.revolutionanalytics.com/snapshot/2021-10-25")
  #install.packages("parallel", repos = "https://mran.revolutionanalytics.com/snapshot/2021-10-25")
  install.packages("/dbfs/FileStore/tables/M4metalearning_0_0_0_9000_tar.gz", repos = NULL)
}
                     ) 
