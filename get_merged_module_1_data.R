# Merges module 1 version 1, module 1 version 2 and part of the participants
# and handles participants who have completed
get_merged_module1_data <- function(project) {
  billing <- project
  recr_M1 <- bq_project_query(
    project,
    query = glue(
      "SELECT token, Connect_ID, d_821247024, d_914594314,  d_827220437, ",
      "d_512820379, d_949302066 , d_517311251  ",
      "FROM  `{project}.FlatConnect.participants_JP` ",
      "WHERE  d_821247024='197316935'"
    )
  )
  recr_m1 <- bq_table_download(recr_M1, bigint = "integer64")
  cnames <- names(recr_m1)
  
  # Check that it doesn't match any non-number
  numbers_only <- function(x) ! grepl("\\D", x)
  
  # to check variables in recr_noinact_wl1
  for (i in 1:length(cnames)) {
    varname <- cnames[i]
    var <- pull(recr_m1, varname)
    recr_m1[, cnames[i]] <- 
      ifelse(numbers_only(var), as.numeric(as.character(var)), var)
  }
  
  sql_M1_1 <- bq_project_query(project,
                               query = glue("SELECT * FROM ",
                                       "`{project}.FlatConnect.module1_v1_JP`"))
  sql_M1_2 <- bq_project_query(project,
                               query = glue("SELECT * FROM ",
                                       "`{project}.FlatConnect.module1_v2_JP`"))
  M1_V1 <- bq_table_download(sql_M1_1, bigint = "integer64") 
  M1_V2 <- bq_table_download(sql_M1_2, bigint = "integer64")
  
  # Check that it doesn't match any non-number
  numbers_only <- function(x)
    ! grepl("\\D", x)
  mod1_v1 <- M1_V1
  cnames <- names(M1_V1)
  ###to check variables and convert to numeric
  for (i in 1:length(cnames)) {
    varname <- cnames[i]
    var <- pull(mod1_v1, varname)
    mod1_v1[, cnames[i]] <-
      ifelse(numbers_only(var), as.numeric(as.character(var)), var)
  }
  
  mod1_v2 <- M1_V2
  cnames <- names(M1_V2)
  ###to check variables and convert to numeric
  for (i in 1:length(cnames)) {
    varname <- cnames[i]
    var <- pull(mod1_v2, varname)
    mod1_v2[, cnames[i]] <-
      ifelse(numbers_only(var), as.numeric(as.character(var)), var)
  }
  
  M1_V1.var <- colnames(M1_V1)
  M1_V2.var <- colnames(M1_V2)
  var.matched <- M1_V1.var[which(M1_V1.var %in% M1_V2.var)]
  
  V1_only_vars <-
    colnames(M1_V1)[colnames(M1_V1) %!in% var.matched]
  V2_only_vars <-
    colnames(M1_V2)[colnames(M1_V2) %!in% var.matched]
  
  common.IDs <-
    M1_V1$Connect_ID[M1_V1$Connect_ID %in% M1_V2$Connect_ID]
  M1_V1_common <- mod1_v1[, var.matched]
  
  M1_V2_common <- mod1_v2[, var.matched]
  M1_V1_common$version <- 1
  M1_V2_common$version <- 2
  
  M1_common  <- rbind(M1_V1_common, M1_V2_common)
  
  m1_v1_only <- mod1_v1[, c("Connect_ID", V1_only_vars)] #230 vars
  m1_v2_only <- mod1_v2[, c("Connect_ID", V2_only_vars)] #255 vars
  m1_v1_only$version <- 1
  m1_v2_only$version <- 2
  
  m1_common <- rbind(M1_V1_common, M1_V2_common)
  m1_common_v1 <-
    merge(m1_common,
          m1_v1_only,
          by = c("Connect_ID", "version"),
          all.x = TRUE)
  m1_combined_v1v2 <-
    merge(
      m1_common_v1,
      m1_v2_only,
      by = c("Connect_ID", "version"),
      all.x = TRUE
    )
  
  ##ALL participants, including duplicates to check
  m1_complete <-
    m1_combined_v1v2[
      which(m1_combined_v1v2$Connect_ID %in% 
            recr_m1$Connect_ID[which(recr_m1$d_949302066 == 231311385)]), ] 
  m1_complete <- m1_complete %>% arrange(desc(version))
  
  m1_complete_nodup <-
    m1_complete[!duplicated(m1_complete$Connect_ID), ]
  
  ### To Join the Participants table
  parts <-
    glue(
      "SELECT Connect_ID, token, d_512820379, d_471593703, state_d_934298480, ",
      "d_230663853, d_335767902, d_982402227, d_919254129, d_699625233, ",
      "d_564964481, d_795827569, d_544150384, d_371067537, d_430551721, ",
      "d_821247024, d_914594314,  d_827220437, d_949302066 , d_517311251, ",
      "d_205553981, d_117249500  ",
      "FROM `{project}.FlatConnect.participants_JP` ",
      "where Connect_ID IS NOT NULL"
    )
  parts_table <- bq_project_query(project, parts)
  parts_data <- bq_table_download(parts_table, bigint = "integer64")
  
  ###need to convert type- m1... is double and parts is character
  parts_data$Connect_ID <- as.numeric(parts_data$Connect_ID) 
  
  merged <- left_join(m1_complete_nodup, parts_data, by = "Connect_ID")
  merged
}