# Databricks notebook source
dbutils.fs.put(file = "dbfs:/FileStore/init_scripts/pl_sm_init.R", 
         contents = 'local({r <- getOption("repos")
       r["CRAN"] <- "http://cran.r-project.org" 
       options(repos=r)
})
install.packages("forecast", quiet = TRUE)
install.packages("Mcomp", quiet = TRUE)
install.packages("/dbfs/FileStore/tables/M4metalearning_0_0_0_9000_tar.gz", repos = NULL, quiet = TRUE)
devtools::install_github("pmontman/customxgboost", quiet = TRUE)
install.packages("tsfeatures", quiet = TRUE)
install.packages("smooth", quiet = TRUE)
#devtools::install_version("rstantools") 
#Sys.setenv(DOWNLOAD_STATIC_LIBV8 = 1)
#remotes::install_github("jeroen/V8")
#install.packages("prophet")
install.packages("mlflow")',
overwrite = TRUE)

# COMMAND ----------

dbutils.fs.put(file = "dbfs:/FileStore/init_scripts/pl_sm_init.sh", 
               contents = "
#!/bin/bash
Rscript --verbose /dbfs/FileStore/init_scripts/pl_sm_init.R", overwrite = TRUE)

# COMMAND ----------

dbutils.fs.put(file = "dbfs:/FileStore/init_scripts/pn_dc_sm_init.R", 
         contents = 'local({r <- getOption("repos")
       r["CRAN"] <- "http://cran.r-project.org" 
       options(repos=r)
})
install.packages("forecast")
install.packages("Mcomp")
install.packages("/dbfs/FileStore/tables/M4metalearning_0_0_0_9000_tar.gz", repos = NULL)
devtools::install_github("pmontman/customxgboost")
install.packages("tsfeatures")
install.packages("smooth")
install.packages("mlflow")',
overwrite = TRUE)

# COMMAND ----------

dbutils.fs.put(file = "dbfs:/FileStore/init_scripts/pn_dc_sm_init.sh", 
               contents = "
#!/bin/bash
Rscript --verbose /dbfs/FileStore/init_scripts/pn_dc_sm_init.R", overwrite = TRUE)