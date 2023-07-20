source("get_merged_module_1_data.R")

get_merged_module_2_data <- function(project) {
# This function merges Module 2 V1/V2 data with Module 1 V1/V2 Data. 
# Kelsey and Jing wrote most of the code and Jake Peters modified it to run 
# reliably as a function within the QAQC pipeline. 
  
  ## Load data.table package
  require(data.table)
  require(bigrquery)
  require(tidyr)
  require(glue)
  
  billing <- project
  
  ### Define Helper Functions
  
  # Define a "not in" function
  `%!in%` <- function(x,y){ !`%in%`(x,y) }
  
  # Check that it doesn't match any non-number
  numbers_only <- function(x) ! grepl("\\D", x)
  
  dictionary <- rio::import("https://episphere.github.io/conceptGithubActions/aggregate.json", format = "json")
  
  dd <- dplyr::bind_rows(dictionary,.id="CID")
  dd <-rbindlist(dictionary,fill=TRUE,use.names=TRUE,idcol="CID")
  dd$`Variable Label`[is.na(dd$`Variable Label`)] <- replace_na(dd$'Variable Name')
  
  dd <- as.data.frame.matrix(do.call("rbind",dictionary))
  dd$CID <- rownames(dd)
  #https://shaivyakodan.medium.com/7-useful-r-packages-for-analysis-7f60d28dca98
  devtools::install_github("tidyverse/reprex")
  
  ##517311251 Date/time Status of Completion of Background and Overall Health                         SrvBOH_TmComplete_v1r0
  ##949302066 Flag for Baseline Module Background and Overall Health                        SrvBOH_BaseStatus_v1r0
  q <- glue("SELECT token,Connect_ID, d_821247024, d_914594314, ",
             "d_827220437, d_512820379, d_536735468 , d_517311251  ",
             "FROM  `{project}.FlatConnect.participants_JP` ",
             "WHERE  d_821247024='197316935' AND d_831041022='104430631'")
  recr_m2 <- bq_project_query(project, query=q)
  recr_m2 <- bq_table_download(recr_m2,bigint = "integer64")
  cnames <- names(recr_m2)

  # to check variables in recr_noinact_wl1
  for (i in 1: length(cnames)){
    varname <- cnames[i]
    var<-pull(recr_m2,varname)
    recr_m2[,cnames[i]] <- ifelse(numbers_only(var), as.numeric(as.character(var)), var)
  }
  
  sql_m2_1 <- bq_project_query(project, query=glue("SELECT * FROM `{project}.FlatConnect.module2_v1_JP` where Connect_ID is not null"))
  sql_m2_2 <- bq_project_query(project, query=glue("SELECT * FROM `{project}.FlatConnect.module2_v2_JP` where Connect_ID is not null"))
  
  
  M2_V1 <- bq_table_download(sql_m2_1,bigint = "integer64") #1436 #1436 vars: 1507 01112023
  M2_V2 <- bq_table_download(sql_m2_2,bigint = "integer64") #2333 #3033 01112023 var:1531 #6339 obs 1893 vars 05022023
  
  mod2_v1 <- M2_V1
  cnames <- names(M2_V1)
  ###to check variables and convert to numeric
  for (i in 1: length(cnames)){
    varname <- cnames[i]
    var<-pull(mod2_v1,varname)
    mod2_v1[,cnames[i]] <- ifelse(numbers_only(var), as.numeric(as.character(var)), var)
  }
  mod2_v2 <- M2_V2
  cnames <- names(M2_V2)
  ###to check variables and convert to numeric
  for (i in 1: length(cnames)){
    varname <- cnames[i]
    var<-pull(mod2_v2,varname)
    mod2_v2[,cnames[i]] <- ifelse(numbers_only(var), as.numeric(as.character(var)), var)
  }
  
  M2_V1.var <- colnames(M2_V1)
  M2_V2.var <- colnames(M2_V2)
  var.matched <- M2_V1.var[which(M2_V1.var %in% M2_V2.var)]
  length(var.matched)  #1275 #1278 vars 01112023 #1348 vars 05022023
  
  V1_only_vars <- colnames(M2_V1)[colnames(M2_V1) %!in% var.matched] #232 #229 01112023 #159 05022023
  V2_only_vars <- colnames(M2_V2)[colnames(M2_V2) %!in% var.matched] #253 #253 01112023 #545 05022023
  
  length(M2_V1$Connect_ID[M2_V1$Connect_ID %in% M2_V2$Connect_ID])
  #[1] 59 with the completion of two versions of module2
  #[1] 62 with completing both versions of M1 ###double checked 03/28/2023
  #68 double checked 05/02/2023
  
  common.IDs <- M2_V1$Connect_ID[M2_V1$Connect_ID %in% M2_V2$Connect_ID]
  M2_V1_common <- mod2_v1[,var.matched]
  
  M2_V2_common <- mod2_v2[,var.matched]
  M2_V1_common$version <- 1
  M2_V2_common$version <- 2
  
  ##to check the completion of M1 among these duplicates
  partM2_dups <- recr_m2[which(recr_m2$Connect_ID %in% common.IDs),]
  table(partM2_dups$d_536735468)
  
  M2_common  <- rbind(M2_V1_common, M2_V2_common) #including 136 duplicates (version 1 and version 2) from 68 participants 05022023
  #M2_response <- matrix(data=NA, nrow=118, ncol=967)
  
  M2_v1_only <- mod2_v1[,c("Connect_ID", V1_only_vars)] #230 vars 03282023 #160 vars 05/02/2023
  M2_v2_only <- mod2_v2[,c("Connect_ID", V2_only_vars)] #255 vars 03282023 #546 vars 05/02/2023
  M2_v1_only$version <- 1
  M2_v2_only$version <- 2
  #for (i in 1:length)
  ##to check the completion in each version
  length(recr_m2$Connect_ID[which(recr_m2$Connect_ID %in% M2_v1_only$Connect_ID & recr_m2$d_536735468 ==231311385)]) #1364 03282023 # 1370 05022023
  length(recr_m2$Connect_ID[which(recr_m2$Connect_ID %in% M2_v2_only$Connect_ID & recr_m2$d_536735468 ==231311385)]) #4870 03282023 # 5731 05022023
  
  #library(janitor)
  
  M2_common <- rbind(M2_V1_common,M2_V2_common)
  M2_common_v1 <- base::merge(M2_common, M2_v1_only, by=c("Connect_ID","version"),all.x=TRUE)
  M2_combined_v1v2 <- base::merge(M2_common_v1,M2_v2_only,by=c("Connect_ID","version"),all.x=TRUE)
  M2_complete <- M2_combined_v1v2[which(M2_combined_v1v2$Connect_ID %in% recr_m2$Connect_ID[which(recr_m2$d_536735468 ==231311385 )]),] #7289 including duplicates 05022023
  
  M2_complete <- M2_complete %>% arrange(desc(version))
  
  
  M2_complete_nodup <- M2_complete[!duplicated(M2_complete$Connect_ID),]
  table(M2_complete_nodup$version)
  
  
  M2_complete_nodup$Connect_ID <- as.numeric(M2_complete_nodup$Connect_ID)
  
  ##### Get Module 1 Data #####
  # Note, this returns M1V1, M1V2 and some Recruitment data
  merge_m1 <- get_merged_module_1_data(project)
  
  ##### Merge Module 1 and Module 2 Data #####
  data <- left_join(M2_complete_nodup, merge_m1, by="Connect_ID")
  data
}


# bq_auth(
#   email = "jake.peters@nih.gov",
#   scopes = c("https://www.googleapis.com/auth/bigquery",
#              "https://www.googleapis.com/auth/cloud-platform"),
#   cache = gargle::gargle_oauth_cache(),
#   use_oob = gargle::gargle_oob_default(),
#   token = token_fetch())
# 
# data <- get_merged_module_2_data("nih-nci-dceg-connect-prod-6d04")