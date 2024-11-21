get_merged_module_2_data <- function(project) {
# This function merges Module 2 V1/V2 data with Module 1 V1/V2 Data.
# Kelsey and Jing wrote most of the code and Jake Peters modified it to run
# reliably as a function within the QAQC pipeline.
  library(rio)
  library(dplyr)
  library(data.table)
  library(glue)
  library(bigrquery)

  `%!in%` <- function(x,y){ !`%in%`(x,y) }

  billing <- project

  ##517311251 Date/time Status of Completion of Background and Overall Health [SrvBOH_TmComplete_v1r0]
  ##949302066 Flag for Baseline Module Background and Overall Health          [SrvBOH_BaseStatus_v1r0]
  recr_M1 <- bq_project_query(
    project,
    query=glue(
      "SELECT token, Connect_ID, d_821247024, d_914594314,  d_827220437,
              d_512820379, d_949302066 , d_517311251
       FROM  `{project}.FlatConnect.participants_JP`
       WHERE  d_821247024='197316935'")
  )
  recr_m1 <- bq_table_download(recr_M1, bigint = "integer64")
  cnames <- names(recr_m1)
  # Check that it doesn't match any non-number
  numbers_only <- function(x) !grepl('\\D', x)
  # to check variables in recr_noinact_wl1
  for (i in 1: length(cnames)){
    varname <- cnames[i]
    var<-pull(recr_m1,varname)
    recr_m1[,cnames[i]] <- ifelse(numbers_only(var), as.numeric(as.character(var)), var)
  }


  #Only these variables carry over from mod1 into mod2:
  #sex, sex2, gen, work
  #Going to see if only these are selected, if it will allow the automation to run
  sql_M1_1 <- bq_project_query(
    project,
    query = glue(
      "SELECT Connect_ID, D_407056417, D_750420077_D_846483618,
              D_750420077_D_505282171, D_750420077_D_578416151, D_750420077_D_434651539,
              D_750420077_D_108025529, D_289664241_D_289664241, D_613744428
       FROM `{project}.FlatConnect.module1_v1_JP`
       WHERE Connect_ID IS NOT NULL")
  )
  sql_M1_2 <- bq_project_query(
    project,
    query = glue(
      "SELECT Connect_ID, D_407056417, D_750420077_D_582784267, D_750420077_D_751402477,
              D_750420077_D_700100953, D_750420077_D_846483618, D_750420077_D_505282171,
              D_750420077_D_578416151, D_750420077_D_434651539, D_750420077_D_108025529,
              D_289664241_D_289664241, D_613744428
       FROM `{project}.FlatConnect.module1_v2_JP`
       WHERE Connect_ID IS NOT NULL")
  )

  M1_V1 <- bq_table_download(sql_M1_1,bigint = "integer64")
  M1_V2 <- bq_table_download(sql_M1_2,bigint = "integer64")

  mod1_v1 <- M1_V1
  cnames <- names(M1_V1)
  ###to check variables and convert to numeric
  for (i in 1: length(cnames)){
    varname <- cnames[i]
    var<-pull(mod1_v1,varname)
    mod1_v1[,cnames[i]] <- ifelse(numbers_only(var), as.numeric(as.character(var)), var)
  }
  mod1_v2 <- M1_V2
  cnames <- names(M1_V2)
  ###to check variables and convert to numeric
  for (i in 1: length(cnames)){
    varname <- cnames[i]
    var <- pull(mod1_v2,varname)
    mod1_v2[,cnames[i]] <- ifelse(numbers_only(var), as.numeric(as.character(var)), var)
  }

  M1_V1.var <- colnames(M1_V1)
  M1_V2.var <- colnames(M1_V2)
  var.matched <- M1_V1.var[which(M1_V1.var %in% M1_V2.var)]
  length(var.matched)

  V1_only_vars <- colnames(M1_V1)[colnames(M1_V1) %!in% var.matched]
  V2_only_vars <- colnames(M1_V2)[colnames(M1_V2) %!in% var.matched]

  length(M1_V1$Connect_ID[M1_V1$Connect_ID %in% M1_V2$Connect_ID])

  common.IDs <- M1_V1$Connect_ID[M1_V1$Connect_ID %in% M1_V2$Connect_ID]
  M1_V1_common <- mod1_v1[,var.matched]

  M1_V2_common <- mod1_v2[,var.matched]
  M1_V1_common$version <- 1
  M1_V2_common$version <- 2

  ##to check the completion of M1 among these duplicates
  partM1_dups <- recr_m1[which(recr_m1$Connect_ID %in% common.IDs),]
  table(partM1_dups$d_949302066)

  M1_common  <- rbind(M1_V1_common, M1_V2_common)

  m1_v1_only <- mod1_v1[,c("Connect_ID", V1_only_vars)]
  m1_v2_only <- mod1_v2[,c("Connect_ID", V2_only_vars)]
  m1_v1_only$version <- 1
  m1_v2_only$version <- 2

  length(recr_m1$Connect_ID[which(recr_m1$Connect_ID %in% m1_v1_only$Connect_ID & recr_m1$d_949302066 ==231311385)])
  length(recr_m1$Connect_ID[which(recr_m1$Connect_ID %in% m1_v2_only$Connect_ID & recr_m1$d_949302066 ==231311385)])

  m1_common <- rbind(M1_V1_common,M1_V2_common)
  m1_common_v1 <- base::merge(m1_common, m1_v1_only, by=c("Connect_ID","version"),all.x=TRUE)
  m1_combined_v1v2 <- base::merge(m1_common_v1,m1_v2_only,by=c("Connect_ID","version"),all.x=TRUE)
  m1_complete <- m1_combined_v1v2[which(m1_combined_v1v2$Connect_ID %in% recr_m1$Connect_ID[which(recr_m1$d_949302066 ==231311385 )]),]

  m1_complete <- m1_complete %>% arrange(desc(version))
  m1_complete_nodup <- m1_complete[!duplicated(m1_complete$Connect_ID),]



  parts <- glue("SELECT
                Connect_ID, token, d_832139544, d_541836531, d_536735468, d_827220437
              FROM `{project}.FlatConnect.participants_JP`
              WHERE Connect_ID IS NOT NULL
                AND (d_512820379='486306141' OR d_512820379='854703046')
                AND d_536735468='231311385'"
  )

  parts_table <- bq_project_query(project, parts)
  parts_data <- bq_table_download(parts_table, bigint = "integer64",n_max = Inf, page_size = 10000)

  parts_data$Connect_ID <- as.numeric(parts_data$Connect_ID)

  merged= left_join(m1_complete_nodup, parts_data, by="Connect_ID")




  recr_m2 <- bq_project_query(project,
                              query=glue("SELECT
                token, Connect_ID, d_821247024, d_914594314, d_827220437,
                d_512820379, d_536735468, d_517311251
             FROM `{project}.FlatConnect.participants_JP`
             WHERE  d_821247024='197316935'"
                              )
  )

  recr_m2 <- bq_table_download(recr_m2,bigint = "integer64")
  cnames <- names(recr_m2)

  # Check that it doesn't match any non-number
  numbers_only <- function(x) !grepl("\\D", x)
  # to check variables in recr_noinact_wl1
  for (i in 1: length(cnames)){
    varname <- cnames[i]
    var<-pull(recr_m2,varname)
    recr_m2[,cnames[i]] <- ifelse(numbers_only(var), as.numeric(as.character(var)), var)
  }

  sql_m2_1 <- bq_project_query(project, query=glue("SELECT * FROM `{project}.FlatConnect.module2_v1_JP` WHERE Connect_ID IS NOT NULL"))
  sql_m2_2 <- bq_project_query(project, query=glue("SELECT * FROM `{project}.FlatConnect.module2_v2_JP` WHERE Connect_ID IS NOT NULL"))

  M2_V1 <- bq_table_download(sql_m2_1, bigint="integer64")
  M2_V2 <- bq_table_download(sql_m2_2, bigint="integer64")

  mod2_v1 <- M2_V1
  cnames <- names(M2_V1)
  ###to check variables and convert to numeric
  for (i in 1: length(cnames)){
    varname <- cnames[i]
    var <- pull(mod2_v1,varname)
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
  length(var.matched)

  V1_only_vars <- colnames(M2_V1)[colnames(M2_V1) %!in% var.matched]
  V2_only_vars <- colnames(M2_V2)[colnames(M2_V2) %!in% var.matched]

  length(M2_V1$Connect_ID[M2_V1$Connect_ID %in% M2_V2$Connect_ID])


  common.IDs <- M2_V1$Connect_ID[M2_V1$Connect_ID %in% M2_V2$Connect_ID]
  M2_V1_common <- mod2_v1[,var.matched]

  M2_V2_common <- mod2_v2[,var.matched]
  M2_V1_common$version <- 1
  M2_V2_common$version <- 2

  ##to check the completion of M1 among these duplicates
  partM2_dups <- recr_m2[which(recr_m2$Connect_ID %in% common.IDs),]
  table(partM2_dups$d_536735468)

  M2_common  <- rbind(M2_V1_common, M2_V2_common)

  M2_v1_only <- mod2_v1[,c("Connect_ID", V1_only_vars)]
  M2_v2_only <- mod2_v2[,c("Connect_ID", V2_only_vars)]
  M2_v1_only$version <- 1
  M2_v2_only$version <- 2

  ##to check the completion in each version
  length(recr_m2$Connect_ID[which(recr_m2$Connect_ID %in% M2_v1_only$Connect_ID & recr_m2$d_536735468 ==231311385)])
  length(recr_m2$Connect_ID[which(recr_m2$Connect_ID %in% M2_v2_only$Connect_ID & recr_m2$d_536735468 ==231311385)])


  M2_common <- rbind(M2_V1_common,M2_V2_common)
  M2_common_v1 <- base::merge(M2_common, M2_v1_only, by=c("Connect_ID","version"),all.x=TRUE)
  M2_combined_v1v2 <- base::merge(M2_common_v1,M2_v2_only,by=c("Connect_ID","version"),all.x=TRUE)
  M2_complete <- M2_combined_v1v2[which(M2_combined_v1v2$Connect_ID %in% recr_m2$Connect_ID[which(recr_m2$d_536735468 ==231311385 )]),]

  M2_complete <- M2_complete %>% arrange(desc(version))


  M2_complete_nodup <- M2_complete[!duplicated(M2_complete$Connect_ID),]
  table(M2_complete_nodup$version)


  M2_complete_nodup$Connect_ID <- as.numeric(M2_complete_nodup$Connect_ID)


  module2= left_join(M2_complete_nodup, merged,  by="Connect_ID")

  data_tib_m2 <- as_tibble(module2)
  return(data_tib_m2)
}
