get_merged_module_1_data <- function(project) {
  # Merges module 1 version 1, module 1 version 2 and part of the participants
  # and handles participants who have completed.
  
  billing <- project
  
  ### Define Helper Functions
  
  # Define a "not in" function
  `%!in%` <- function(x,y){ !`%in%`(x,y) }
  
  # Check that it doesn't match any non-number
  numbers_only <- function(x) ! grepl("\\D", x)
  
  ### Get both versions of Recruitment data for Module 1 from BQ
  ##517311251 Date/time Status of Completion of Background and Overall Health SrvBOH_TmComplete_v1r0
  ##949302066 Flag for Baseline Module Background and Overall Health SrvBOH_BaseStatus_v1r0
  sql_rec_m1 <- glue("SELECT token, Connect_ID, d_821247024, d_914594314,",
                     "d_827220437, d_512820379, d_949302066 , d_517311251,",
                     "d_205553981 ",
                     "FROM  `{project}.FlatConnect.participants_JP` ",
                     "WHERE  Connect_ID IS NOT NULL AND d_821247024='197316935' AND d_831041022='104430631'")
  query_recr_m1 <- bq_project_query(project, query=sql_rec_m1)
  recr_m1 <- bq_table_download(query_recr_m1,bigint = "integer64")
  cnames <- names(recr_m1)
  
  ### Check Variables and Convert to Numeric
  
  for (i in 1: length(cnames)){
    varname <- cnames[i]
    var     <- pull(recr_m1,varname)
    recr_m1[,cnames[i]] <- 
      ifelse(numbers_only(var), as.numeric(as.character(var)), var)
  }
  
  ### Get both versions of Module 1 from BQ
  sql_m1_v1   <- glue("SELECT * FROM `{project}.FlatConnect.module1_v1_JP` ",
                      "where Connect_ID is not null")
  query_m1_v1 <- bq_project_query(project, query=sql_m1_v1)
  M1_V1 <- bq_table_download(query_m1_v1,bigint = "integer64")
  
  sql_m1_v2   <- glue("SELECT * FROM `{project}.FlatConnect.module1_v2_JP` ",
                      "where Connect_ID is not null")
                      query_m1_v2 <- bq_project_query(project, query=sql_m1_v2)
                      M1_V2 <- bq_table_download(query_m1_v2,bigint = "integer64")
                      
                      # Check Variables and Convert to Numeric
                      mod1_v1 <- M1_V1
                      cnames <- names(M1_V1)
                      for (i in 1: length(cnames)){
                        varname <- cnames[i]
                        var<-pull(mod1_v1,varname)
                        mod1_v1[,cnames[i]] <- 
                          ifelse(numbers_only(var), as.numeric(as.character(var)), var)
                      }
                      
                      mod1_v2 <- M1_V2
                      cnames <- names(M1_V2)
                      for (i in 1: length(cnames)){
                        varname <- cnames[i]
                        var<-pull(mod1_v2,varname)
                        mod1_v2[,cnames[i]] <-
                          ifelse(numbers_only(var), as.numeric(as.character(var)), var)
                      }
                      
                      
                      ### Begin Merging Steps
                      M1_V1.var <- colnames(M1_V1)
                      M1_V2.var <- colnames(M1_V2)
#                      print('M1_V1 columns: ', M1_V1.var)
#                      print('M1_V1 columns: ', M1_V1.var)
                      
                      var.matched <- M1_V1.var[which(M1_V1.var %in% M1_V2.var)]
                      length(var.matched)  
                      
                      V1_only_vars <- colnames(M1_V1)[colnames(M1_V1) %!in% var.matched] 
                      V2_only_vars <- colnames(M1_V2)[colnames(M1_V2) %!in% var.matched] 
                      
                      length(M1_V1$Connect_ID[M1_V1$Connect_ID %in% M1_V2$Connect_ID])
                      #[1] 59 with the completion of two versions of Module1
                      #[1] 62 with completing both versions of M1 ###double checked 03/28/2023
                      #68 double checked 05/02/2023
                      
                      common.IDs <- M1_V1$Connect_ID[M1_V1$Connect_ID %in% M1_V2$Connect_ID]
                      M1_V1_common <- mod1_v1[,var.matched]
                      
                      M1_V2_common <- mod1_v2[,var.matched]
                      M1_V1_common$version <- 1
                      M1_V2_common$version <- 2
                      
                      ##to check the completion of M1 among these duplicates
                      partM1_dups <- recr_m1[which(recr_m1$Connect_ID %in% common.IDs),]
                      table(partM1_dups$d_949302066)
                      
                      M1_common  <- rbind(M1_V1_common, M1_V2_common) 
                      #including 136 duplicates (version 1 and version 2) 
                      # from 68 participants 05022023
                      #M1_response <- matrix(data=NA, nrow=118, ncol=967)
                      
                      m1_v1_only <- mod1_v1[,c("Connect_ID", V1_only_vars)] #230 vars 03282023 #160 vars 05/02/2023
                      m1_v2_only <- mod1_v2[,c("Connect_ID", V2_only_vars)] #255 vars 03282023 #546 vars 05/02/2023
                      m1_v1_only$version <- 1
                      m1_v2_only$version <- 2
                      
                      ##to check the completion in each version
                      # length(
                      #   recr_m1$Connect_ID[
                      #     which(recr_m1$Connect_ID %in% m1_v1_only$Connect_ID & 
                      #           recr_m1$d_949302066 == 231311385)]) #1364 03282023 # 1370 05022023
                      # length(recr_m1$Connect_ID[
                      #   which(recr_m1$Connect_ID %in% m1_v2_only$Connect_ID & 
                      #           recr_m1$d_949302066 ==231311385)]) #4870 03282023 # 5731 05022023
                      
                      #library(janitor)
                      
                      m1_common        <- rbind(M1_V1_common,M1_V2_common)
                      m1_common_v1     <- base::merge(m1_common, m1_v1_only, 
                                                by=c("Connect_ID","version"), all.x=TRUE)
                      m1_combined_v1v2 <- base::merge(m1_common_v1, m1_v2_only,
                                                by=c("Connect_ID","version"), all.x=TRUE)
                      m1_complete      <- m1_combined_v1v2[which(
                        m1_combined_v1v2$Connect_ID %in% 
                          recr_m1$Connect_ID[which(recr_m1$d_949302066 ==231311385 )]),] 
                      #7289 including duplicates 05022023
                      
                      m1_complete <- m1_complete %>% arrange(desc(version))
                      
                      
                      m1_complete_nodup <- m1_complete[!duplicated(m1_complete$Connect_ID),]
                      
                      ### Join the Participants table to Merged Module 1 Data
                      parts <-
                        glue(
                          "SELECT Connect_ID, token, d_512820379, d_471593703, ",
                          "state_d_934298480, d_230663853, d_335767902,",
                          "d_982402227, d_919254129, d_699625233, d_564964481, ",
                          "d_795827569, d_544150384, d_371067537, d_430551721, ",
                          "d_821247024, d_914594314, d_827220437, d_949302066, ",
                          "d_517311251, d_205553981, d_117249500, ",
                          "FROM `{project}.FlatConnect.participants_JP` ",
                          "WHERE Connect_ID IS NOT NULL"
                        )
                      parts_table <- bq_project_query(project, parts)
                      parts_data <- bq_table_download(parts_table, 
                                                      bigint="integer64")
                      
                      #need to convert type- m1 is double & parts is character
                      parts_data$Connect_ID <- as.numeric(parts_data$Connect_ID) 
                      
                      merged <- left_join(m1_complete_nodup, parts_data, 
                                          by = "Connect_ID")
                      merged
}