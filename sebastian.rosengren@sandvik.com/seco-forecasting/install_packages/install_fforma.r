# Databricks notebook source
library(tidyverse)

# COMMAND ----------

install.packages("forecast")
install.packages("Mcomp")
install.packages("/dbfs/FileStore/tables/M4metalearning_0_0_0_9000_tar.gz", repos = NULL)
devtools::install_github("pmontman/customxgboost")
install.packages("tsfeatures")
install.packages("smooth")

# COMMAND ----------

library(sparklyr)
sc <- spark_connect(method = "databricks")