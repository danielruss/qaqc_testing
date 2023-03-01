# test
library(tidyverse)
library(bigrquery)
library(tidyverse)
library(rlang)
library(plumber)
library(googleCloudStorageR)
library(gargle)
library(readxl)

# #* heartbeat...
# #* @get /
# #* @post /
# function(){
#   return("alive")
# }
# 
# #* Runs STAGE QAQC
# #* @get /qaqc-recruitment
# #* @post /qaqc-recruitment
# function() {

# Dictionary provided by Nicole on Jan 19, 2023:
#dictionary <- rio::import("https://github.com/episphere/conceptGithubActions/blob/master/aggregate.json",format = "json")
dictionary <- rio::import("https://raw.githubusercontent.com/episphere/conceptGithubActions/master/aggregate.json",format = "json")

#dl <- map(dictionary,"Variable Label") %>% compact()
dl <-  dictionary %>% map(~.x[["Variable Label"]] %||% .x[["Variable Name"]]) %>% compact()
dictionary_lookup <- function(x){
  x=as.list(x)
  skel <- as.relistable(x)
  x <- unlist(x)
  
  x <- x %>% str_remove_all("^d_|^state_d_|(?<=_)d_") %>% 
    map(~dl[[.x]]) %>% 
    #modify_if(is.null,~"Not in Dictionary")
    modify_if(is.null,~"NA")
  x <- relist(x,skel)
  class(x) <- "list"
  # x <- x %>% lapply(unlist) %>% lapply(paste, collapse=(', ')) 
  x
}

# Convert comma sep vals to vector, handling caveats
convertToVector <- function(x){
  if (is.na(x) || nchar(x)==0) return(NA_character_)
  str_trim(unlist(str_split(x,pattern = ",")))
}




#### ==================== Code Really Starts here...
# your %!in% function is not my favorite, but infix operator are cool so +1. I'm surprise 
# your function works.  I changed the syntax a bit to make it slightly more standard. 
# Be careful in using this pipes, which I think are much cooler and easier to read.
`%!in%` <- function(x,y){ !`%in%`(x,y) }
`%in|na%` <- function(x,y){ is.na(x) | x %in% y }
# `%in|fun%` <- function(x,y){
#   if (y[1]=="populated") {
#     return(!is.na(x))
#   } else {
#     return(x %in% y)
#   }
# }

# I'm not really happy with this function...  I would like to pass
# in two vectors values and valid_values. This way we are not locked
# into 3 columns.  Note: NA %in% NA returns true, so if we are cross 
# validating 1 variable, the NAs in v2 and v2 dont effect the results.
crossValidate <- function(value1,valid_values1,value2,valid_values2,value3,valid_values3,value4,valid_values4){
  (value1 %in% valid_values1) & (value2 %in% valid_values2) & (value3 %in% valid_values3) & (value4 %in% valid_values4)
}
# crossValidate <- function(value1,valid_values1,value2,valid_values2,value3,valid_values3,value4,valid_values4){
#   (value1 %in|fun% valid_values1) & (value2 %in|fun% valid_values2) & (value3 %in|fun% valid_values3) & (value4 %in|fun% valid_values4)
# }

## getCIDValues takes a string of a column name (a concept id), and looks up the
## the value in the data tibble.  The issue we need to be careful
## with are the NA's. Dont try to evaluate NAs..
# Note: data is a column of data for a CID from BQ
getCIDValue <- function(cid,data){
  stopifnot(length(cid)==1)
  if (is.na(cid)) {
    return(NA_character_)
  }
  eval_tidy(sym(cid),data)
}

prepare_list_for_report<-function(arg_list){
  # JP NOTE: Guarantee that everything is defined, even if with an empty str
  arg_list$CrossVariableConceptID1 <- arg_list$CrossVariableConceptID1 %||% ""
  arg_list$CrossVariable1Value <- arg_list$CrossVariable1Value %||% ""
  arg_list$CrossVariableConceptValidValue1 <- list(arg_list$CrossVariableConceptID1Value %||% "")
  
  arg_list$CrossVariableConceptID2 <- arg_list$CrossVariableConceptID2 %||% ""
  arg_list$CrossVariable2Value <- arg_list$CrossVariable2Value %||% ""
  arg_list$CrossVariableConceptValidValue2 <- list(arg_list$CrossVariableConceptID2Value %||% "")
  
  arg_list$CrossVariableConceptID3 <- arg_list$CrossVariableConceptID3 %||% ""
  arg_list$CrossVariable3Value <- arg_list$CrossVariable3Value %||% ""
  arg_list$CrossVariableConceptValidValue3 <- list(arg_list$CrossVariableConceptID3Value %||% "")
  
  arg_list$CrossVariableConceptID4 <- arg_list$CrossVariableConceptID4 %||% ""
  arg_list$CrossVariable4Value <- arg_list$CrossVariable4Value %||% ""
  arg_list$CrossVariableConceptValidValue4 <- list(arg_list$CrossVariableConceptID4Value %||% "")
  
  arg_list
}



prepare_report <- function(data,l,ids){
  data %>% 
    mutate(invalid_values=!!sym(l$ConceptID),
           value2="",
           date=l$date,
           ConceptID=l$ConceptID,
           rule_error="",
           rule_label=l$Label,
           ConceptID_lookup=dictionary_lookup(ConceptID),
           ValidValues=list(l$ValidValues),
           ValidValues_lookup=dictionary_lookup(ValidValues),
           CrossVariableConceptID1=l$CrossVariableConceptID1 %||% "",
           CrossVariableConceptID1_lookup=dictionary_lookup(CrossVariableConceptID1),
           CrossVariableConceptValidValue1=list(l$CrossVariableConceptID1Value %||% ""),
           CrossVariableConceptValidValue1_lookup=dictionary_lookup(CrossVariableConceptValidValue1),
           CrossVariable1Value=l$CrossVariable1Value %||% "",
           CrossVariableConceptID2=l$CrossVariableConceptID2 %||% "",
           CrossVariableConceptID2_lookup=dictionary_lookup(CrossVariableConceptID2),
           CrossVariableConceptValidValue2=list(l$CrossVariableConceptID2Value %||% ""),
           CrossVariableConceptValidValue2_lookup=dictionary_lookup(CrossVariableConceptValidValue2),
           CrossVariable2Value=l$CrossVariable2Value %||% "",
           CrossVariableConceptID3=l$CrossVariableConceptID3 %||% "",
           CrossVariableConceptID3_lookup=dictionary_lookup(CrossVariableConceptID3),
           CrossVariableConceptValidValue3=list(l$CrossVariableConceptID3Value %||% ""),
           CrossVariableConceptValidValue3_lookup=dictionary_lookup(CrossVariableConceptValidValue3),
           CrossVariable3Value=l$CrossVariable3Value %||% "",
           CrossVariableConceptID4=l$CrossVariableConceptID4 %||% "",
           CrossVariableConceptID4_lookup=dictionary_lookup(CrossVariableConceptID4),
           CrossVariableConceptValidValue4=list(l$CrossVariableConceptID4Value %||% ""),
           CrossVariableConceptValidValue4_lookup=dictionary_lookup(CrossVariableConceptValidValue4),
           CrossVariable4Value=l$CrossVariable4Value %||% "",
           qc_test=l$Qctype
    )  %>%
    select(Connect_ID,
           token, 
           qc_test,
           rule_label,
           rule_error,
           ConceptID,
           ConceptID_lookup,
           date,
           ValidValues,
           ValidValues_lookup,
           invalid_values,
           CrossVariableConceptID1,
           CrossVariableConceptID1_lookup,
           #CrossVariable1Value,
           CrossVariableConceptValidValue1,
           CrossVariableConceptValidValue1_lookup,
           CrossVariableConceptID2,
           CrossVariableConceptID2_lookup,
           #CrossVariable2Value,
           CrossVariableConceptValidValue2,
           CrossVariableConceptValidValue2_lookup,
           CrossVariableConceptID3,
           CrossVariableConceptID3_lookup,
           #CrossVariable3Value,
           CrossVariableConceptValidValue3,
           CrossVariableConceptValidValue3_lookup,
           CrossVariableConceptID4,
           CrossVariableConceptID4_lookup,
           #CrossVariable4Value,
           CrossVariableConceptValidValue4,
           CrossVariableConceptValidValue4_lookup,
           {{ids}})
}

crossvalidly <- function(f){
  function(data,ids,...){
    l = list(...)  
    
    rules_error <- FALSE
    error_msg <- ""
    tryCatch({
      cid_value <- getCIDValue(l$ConceptID,data)
      xv1_value <- getCIDValue(l$CrossVariableConceptID1,data)
      xv2_value <- getCIDValue(l$CrossVariableConceptID2,data)
      xv3_value <- getCIDValue(l$CrossVariableConceptID3,data)
      xv4_value <- getCIDValue(l$CrossVariableConceptID4,data)
    },error=function(e){
      rules_error <- TRUE
      error_msg <- paste("Rules error: ",e) 
      warning("Caught error during crossvalidation.",
              "\n\tConcept_ID:",l$ConceptID,
              "\n\tXV_ID1:",l$CrossVariableConceptID1,
              "\n\tXV_ID2:",l$CrossVariableConceptID2,
              "\n\tXV_ID3:",l$CrossVariableConceptID3,
              "\n\tXV_ID4:",l$CrossVariableConceptID4)
    })
    
    # JP NOTE: Everything in report rows passed the cross valid, so valid() will only 
    # run on the CIDs that passed the cross validation
    report_rows <- data %>% 
      ## add a column which is the value of the ConceptId of interest...
      mutate(value = cid_value,
             date=l$date,
             CrossVariable1Value=xv1_value,
             CrossVariable2Value=xv2_value,
             CrossVariable3Value=xv3_value,
             CrossVariable4Value=xv4_value) %>%
      filter(
        crossValidate(
          xv1_value,l$CrossVariableConceptID1Value,
          xv2_value,l$CrossVariableConceptID2Value,
          xv3_value,l$CrossVariableConceptID3Value,
          xv4_value,l$CrossVariableConceptID4Value)
      )
    f(report_rows,ids={{ids}},...)
    
  }
}

na_ok <- function(f){
  function(data,ids,...){
    f(data,ids={{ids}},na_ok=TRUE,...) %>%
      filter(!is.na(invalid_values))
  }
}


valid <- function(data,ids,...){
  #message("... Running valid")
  # ok, this is a difficult concept, but the conceptId is passed in as string.
  # if you try to say "is the value of the conceptId that I passed in within a set of valid values
  # you would be testing in the string "d_xxxxxxx" is in the set of value values.  We need to let
  # R know that is a variable name (a symbol).  So we convert the conceptId to a symbol.  Then
  # we need to ask R if the value that the symbol points is in the set of valid values, not the symbol
  # itself.  So the !! dereferences the symbol to the value.
  l=list(...) # Column names that are passed in ...
  
  ## select all the invalid rows. and save it in the "failed" tibble...
  failed <- data %>% filter( !!sym(l$ConceptID) %!in% l$ValidValues)
  
  l <- prepare_list_for_report(l)
  
  ## quoted_conceptId = enquo(conceptId)
  ## for each failed row, get the Ids we want to place in the report...
  ## I also set the test to VALID
  ## you will need to set other values to as needed.
  
  prepare_report(failed,l,{{ids}})
}
crossvalid <- crossvalidly(valid)
na_or_valid <- na_ok(valid)
na_or_crossvalid <- na_ok(crossvalid)

## Check that date is before other date
validBeforeDate <- function(data,ids,...){
  l=list(...)
  
  # get value of cid (should be a date)
  date_to_check <- getCIDValue(l$ConceptID,data)
  # Converting to a date from a string
  date_to_check <- coalesce(
    lubridate::ymd(date_to_check),
    lubridate::ymd_hms(date_to_check) )

  date_from_valid_vals <- getCIDValue(l$ValidValues,data)
  date_from_valid_vals <- coalesce(
    lubridate::ymd(date_from_valid_vals),
    lubridate::ymd_hms(date_from_valid_vals) )
  
  # should fail if NA or if date to check is later than valid values date
  
  failed <- data %>% filter( is.na(date_to_check) | date_to_check >= date_from_valid_vals )
  l <- prepare_list_for_report(l)
  prepare_report(failed,l,{{ids}})
}
na_or_validBeforeDate <- na_ok(validBeforeDate)
crossvalidBeforeDate <- crossvalidly(validBeforeDate)
na_or_crossvalidBeforeDate <- crossvalidly(na_or_validBeforeDate)

# Filter values that are NA
isPopulated <- function(data,ids,...){
  l=list(...) # Column names that are passed in ...
  
  value_to_check <- getCIDValue(l$ConceptID,data)
  failed <- data %>% filter(is.na(value_to_check))
  
  l <- prepare_list_for_report(l)
  prepare_report(failed,l,{{ids}})
}
crossvalidIsPopulated <- crossvalidly(isPopulated)

validDate <- function(data,ids,...){
  #message("... Running ValidDate")
  l=list(...)
  
  symbol_conceptId = sym(l$ConceptID)
  ### check that the format is yyyymmdd
  suppressWarnings(
    failed <- data %>% filter(
      is.na(coalesce(
        lubridate::ymd(!!symbol_conceptId ),
        lubridate::ymd_hms(!!symbol_conceptId) )) )
  )
  # make sure elements needed in the report are not NULL ... 
  l <- prepare_list_for_report(l)
  prepare_report(failed,l,{{ids}})
}
na_or_date <- na_ok(validDate)
crossvalidDate = crossvalidly(validDate)

validDateOnly <- function(data,ids,...){
  l=list(...)
  
  symbol_conceptId = sym(l$ConceptID)
  ### check that the format is yyyymmdd
  suppressWarnings(
    failed <- data %>% filter(is.na(lubridate::ymd(!!symbol_conceptId )))
  )
  
  # make sure elements needed in the report are not NULL ... 
  l <- prepare_list_for_report(l)
  prepare_report(failed,l,{{ids}})
}
validDateTime <- function(data,ids,...){
  #message("... Running ValidDateTime")
  l=list(...)
  
  symbol_conceptId = sym(l$ConceptID)
  ### check that the format is yyyymmdd
  suppressWarnings(
    failed <- data %>% filter(is.na(lubridate::ymd_hms(!!symbol_conceptId )))
  )
  
  # make sure elements needed in the report are not NULL ... 
  l <- prepare_list_for_report(l)
  prepare_report(failed,l,{{ids}})
}
na_or_datetime <- na_ok(validDateTime)

validAge <- function(data,ids,...){
  #message("... Running ValidAge")
  l=list(...)
  
  ### need to make the column an integer and check that the age is between 0-100
  suppressWarnings(
    failed <- data %>% mutate(tmpcol = as.integer(!!sym(l$ConceptID))) %>% 
      filter( is.na(tmpcol) | tmpcol<=0 | tmpcol >=100 )
  )
  # make sure elements needed in the report are not NULL ... 
  l <- prepare_list_for_report(l)
  prepare_report(failed,l,{{ids}})
}
na_or_age <- na_ok(validAge)

notNA <- function(data,ids,...){
  #message("... Running notNA")
  l=list(...)
  failed <- data %>% filter( is.na(!!sym(l$ConceptID)) )
  l <- prepare_list_for_report(l)
  prepare_report(failed,l,{{ids}})
}
crossValid_notNA <- crossvalidly(notNA)

has_n_characters<-function(data,ids,...){
  #message("... Running has_n_characters ")
  l=list(...)
  #message("char len=",l$ValidValues)
  
  failed <- data %>% filter( nchar(!!sym(l$ConceptID)) != as.integer(l$ValidValues))
  l <- prepare_list_for_report(l)
  prepare_report(failed,l,{{ids}})
}
na_or_has_n_characters<-na_ok(has_n_characters)
crossvalid_has_n_characters<- crossvalidly(has_n_characters)

has_less_than_or_equal_n_characters<-function(data,ids,...){
  #message("... Running has_less_or_equal_n_characters ")
  l=list(...)
  #message("char len=",l$ValidValues)
  failed <- data %>% filter( nchar(!!sym(l$ConceptID)) > as.integer(l$ValidValues))
  l <- prepare_list_for_report(l)
  prepare_report(failed,l,{{ids}})
}
na_or_has_less_than_or_equal_n_characters<-na_ok(has_less_than_or_equal_n_characters)
crossvalid_has_less_than_or_equal_n_characters<- crossvalidly(has_less_than_or_equal_n_characters)

# NOTE: This was needed to report bad rules
find_errors <- function(rules,data){
  rules %>% mutate(
    cid_not_in_data=ConceptID %!in% names(data),
    bad_xv1_cid = !CrossVariableConceptID1 %in|na% names(data), 
    bad_xv2_cid = !CrossVariableConceptID2 %in|na% names(data),
    bad_xv3_cid = !CrossVariableConceptID3 %in|na% names(data),
    bad_xv4_cid = !CrossVariableConceptID4 %in|na% names(data),
    error = if_else(cid_not_in_data,"CID not in BQ table",""),
    error = if_else(bad_xv1_cid, paste(error,"Cross Var. CID 1 not in BQ table"),error),
    error = if_else(bad_xv2_cid, paste(error,"Cross Var. CID 2 not in BQ table"),error),
    error = if_else(bad_xv3_cid, paste(error,"Cross Var. CID 3 not in BQ table"),error),
    error = if_else(bad_xv4_cid, paste(error,"Cross Var. CID 4 not in BQ table"),error)
  )  %>% select(!cid_not_in_data:bad_xv4_cid)
}
report_bad_rules <- function(...){
  l=list(...)
  return(tibble(qc_test=l$Qctype,ConceptID=l$ConceptID,date=l$date,ValidValues=list(l$ValidValues),rule_error=l$error,rule_label=l$Label,
                CrossVariableConceptID1=l$CrossVariableConceptID1,CrossVariable1Value="",CrossVariableConceptValidValue1=list(l$CrossVariableConceptID1Value),
                CrossVariableConceptID2=l$CrossVariableConceptID2,CrossVariable2Value="",CrossVariableConceptValidValue2=list(l$CrossVariableConceptID2Value),
                CrossVariableConceptID3=l$CrossVariableConceptID3,CrossVariable3Value="",CrossVariableConceptValidValue3=list(l$CrossVariableConceptID3Value),
                CrossVariableConceptID4=l$CrossVariableConceptID4,CrossVariable4Value="",CrossVariableConceptValidValue4=list(l$CrossVariableConceptID4Value)
  ))
}

runQC <- function(data, rules, QC_report_location,ids){
  run_date=Sys.time()
  
  rules <- find_errors(rules,data)
  bad_rules <- rules %>% filter(nchar(error)>0) 
  filtered_rules <- rules %>% filter(!is.na(Qctype) & nchar(error)==0)
  rules <- filtered_rules %>% select(!error)
  
  bind_rows(
    bad_rules %>% pmap_dfr(report_bad_rules,date=run_date),
    rules %>% filter(Qctype=="valid") %>% pmap_dfr(valid,data=data,ids={{ids}},date=run_date),
    rules %>% filter(Qctype=="valid before date") %>% pmap_dfr(validBeforeDate,data=data,ids={{ids}},date=run_date),
    rules %>% filter(Qctype=="crossvalid before date") %>% pmap_dfr(crossvalidBeforeDate,data=data,ids={{ids}},date=run_date),
    rules %>% filter(Qctype=="NA or valid before date") %>% pmap_dfr(na_or_validBeforeDate,data=data,ids={{ids}},date=run_date),
    rules %>% filter(Qctype=="NA or crossvalid before date") %>% pmap_dfr(na_or_crossvalidBeforeDate,data=data,ids={{ids}},date=run_date),
    rules %>% filter(Qctype=="NA or valid") %>% pmap_dfr(na_or_valid,data=data,ids={{ids}},date=run_date),
    rules %>% filter(Qctype=="is populated") %>% pmap_dfr(isPopulated,data=data,ids={{ids}},date=run_date),
    rules %>% filter(Qctype=="crossValid1") %>% pmap_dfr(crossvalid,data=data,ids={{ids}},date=run_date),
    rules %>% filter(Qctype=="crossValid2") %>% pmap_dfr(crossvalid,data=data,ids={{ids}},date=run_date),
    rules %>% filter(Qctype=="crossValid3") %>% pmap_dfr(crossvalid,data=data,ids={{ids}},date=run_date),
    rules %>% filter(Qctype=="crossValid4") %>% pmap_dfr(crossvalid,data=data,ids={{ids}},date=run_date),
    rules %>% filter(Qctype=="crossValid1 is populated") %>% pmap_dfr(crossvalidIsPopulated,data=data,ids={{ids}},date=run_date),
    rules %>% filter(Qctype=="NA or crossValid1") %>% pmap_dfr(na_or_crossvalid,data=data,ids={{ids}},date=run_date),
    rules %>% filter(Qctype=="NA or crossValid2") %>% pmap_dfr(na_or_crossvalid,data=data,ids={{ids}},date=run_date),
    rules %>% filter(Qctype=="NA or crossValid3") %>% pmap_dfr(na_or_crossvalid,data=data,ids={{ids}},date=run_date),
    rules %>% filter(Qctype=="NA or crossValid4") %>% pmap_dfr(na_or_crossvalid,data=data,ids={{ids}},date=run_date),
    rules %>% filter(Qctype=="crossValid1Date") %>% pmap_dfr(crossvalidDate,data=data,ids={{ids}},date=run_date),
    rules %>% filter(Qctype=="crossValid1NotNA") %>% pmap_dfr(crossValid_notNA,data=data,ids={{ids}},date=run_date),
    rules %>% filter(Qctype=="NA or dateTime") %>% pmap_dfr(na_or_datetime,data=data,ids={{ids}},date=run_date),
    rules %>% filter(Qctype=="NA or date") %>% pmap_dfr(na_or_date,data=data,ids={{ids}},date=run_date),
    rules %>% filter(Qctype=="crossValid1 equal to char()") %>% pmap_dfr(crossvalid_has_n_characters,data=data,ids={{ids}},date=run_date),
    rules %>% filter(Qctype=="NA or equal to char()") %>% pmap_dfr(na_or_has_n_characters,data=data,ids={{ids}},date=run_date),
    rules %>% filter(Qctype=="crossValid1 equal to or less than char()") %>% pmap_dfr(crossvalid_has_less_than_or_equal_n_characters,data=data,ids={{ids}},date=run_date),
    rules %>% filter(Qctype=="NA or equal to or less than char()") %>% pmap_dfr(na_or_has_less_than_or_equal_n_characters,data=data,ids={{ids}},date=run_date)
  )
}





#### YOU WILL WANT TO CHANGE THIS TO TRUE...
loadFromBQ=TRUE
if (loadFromBQ){
  #project <- "nih-nci-dceg-connect-stg-5519" # just the project it gets billed from
  #sql <- "SELECT * FROM `nih-nci-dceg-connect-stg-5519.FlatConnect.participants_JP`"
  # project <- "nih-nci-dceg-connect-prod-6d04"
  # sql <- "SELECT * FROM `nih-nci-dceg-connect-prod-6d04.FlatConnect.participants_JP`" 
  # tb <- bq_project_query(project, sql)
  # data <- bq_table_download(tb, bigint = c("character"))
  
  # Manipulate data here so that it has everything I need to check
  # including if I'm pulling data from multiple tables
  recr_var <- bq_project_query(project, query="SELECT * FROM `nih-nci-dceg-connect-prod-6d04.FlatConnect`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS WHERE table_name='participants_JP'")
  recrvar  <- bigrquery::bq_table_download(recr_var,bigint = "integer64")
  recrvar_d <- recrvar[grepl("d_",recrvar$column_name),]
  
  nvar = floor((length(recrvar_d$column_name))/5) ##to define the number of variables in each sql extract from GCP
  nvar
  
  # Start column for each split data frame
  start = seq(1,length(recrvar_d$column_name),nvar)
  end <- length(recrvar_d$column_name)
  # 
  recrbq <- list()
  
  for (i in (1:length(start)))  {
    select <- paste(recrvar_d$column_name[start[i]:(min(start[i]+nvar-1,end))],collapse=",")
    tmp <- eval(parse(text=paste("bq_project_query(project, query=\"SELECT token,Connect_ID,", select,"FROM `nih-nci-dceg-connect-prod-6d04.FlatConnect.participants_JP` Where d_512820379 != '180583933' \")",sep=" ")))
    
    recrbq[[i]] <- bq_table_download(tmp, bigint="integer64")
  }
  
  data <- recrbq %>% reduce(inner_join, by = c("token","Connect_ID"))
}else{
  #data_file <- "~/test_data.json"
  data_file <- "data.json"
  data <- rio::import(data_file) %>% tibble::as_tibble()
}



## I need to load the rules file....
rules_file <- "qc_rules_2_28_23.xlsx"
rules <- read_excel(rules_file,col_types = 'text') %>% 
  mutate(ValidValues=map(ValidValues,convertToVector),
         CrossVariableConceptID1Value=map(CrossVariableConceptID1Value,convertToVector),
         CrossVariableConceptID2Value=map(CrossVariableConceptID2Value,convertToVector),
         CrossVariableConceptID3Value=map(CrossVariableConceptID3Value,convertToVector),
         CrossVariableConceptID4Value=map(CrossVariableConceptID4Value,convertToVector))

# print( system.time(x <- runQC(data, rules,ids=Connect_ID)) )
x <- runQC(data, rules,ids=Connect_ID)

col_order <- c("Connect_ID", "token", "qc_test", "rule_error", "rule_label", "ConceptID", "ConceptID_lookup",
               "ValidValues", "ValidValues_lookup", "invalid_values",
               "CrossVariableConceptID1", "CrossVariableConceptID1_lookup", "CrossVariableConceptValidValue1", "CrossVariableConceptValidValue1_lookup",
               "CrossVariableConceptID2", "CrossVariableConceptID2_lookup", "CrossVariableConceptValidValue2", "CrossVariableConceptValidValue2_lookup",
               "CrossVariableConceptID3", "CrossVariableConceptID3_lookup", "CrossVariableConceptValidValue3", "CrossVariableConceptValidValue3_lookup",
               "CrossVariableConceptID4", "CrossVariableConceptID4_lookup", "CrossVariableConceptValidValue4", "CrossVariableConceptValidValue4_lookup",
               "date")
x <- x[, col_order]

time_stamp <- gsub("-","_", 
                   gsub(" ","_",
                        gsub(":","_",
                             Sys.time())))

report_fid <- paste0("qc_report_recruitment_prod_",time_stamp,".xlsx")
#report_fid <- paste0("qc_report_recruitment_stg_",time_stamp,".xlsx")
x %>% mutate(ConceptID_lookup = map_chr(ConceptID_lookup,paste,collapse=", "),
             ValidValues = map_chr(ValidValues,paste,collapse = ", "),
             ValidValues_lookup = map_chr(ValidValues_lookup,paste,collapse = ", "),
             CrossVariableConceptID1= map_chr(CrossVariableConceptID1,paste,collapse = ", "),
             CrossVariableConceptID1_lookup= map_chr(CrossVariableConceptID1_lookup,paste,collapse = ", "),
             CrossVariableConceptValidValue1= map_chr(CrossVariableConceptValidValue1,paste,collapse = ", "),
             CrossVariableConceptValidValue1_lookup= map_chr(CrossVariableConceptValidValue1_lookup,paste,collapse = ", "),
             CrossVariableConceptID2= map_chr(CrossVariableConceptID2,paste,collapse = ", "),
             CrossVariableConceptID2_lookup= map_chr(CrossVariableConceptID2_lookup,paste,collapse = ", "),
             CrossVariableConceptValidValue2= map_chr(CrossVariableConceptValidValue2,paste,collapse = ", "),
             CrossVariableConceptValidValue2_lookup= map_chr(CrossVariableConceptValidValue2_lookup,paste,collapse = ", "),
             CrossVariableConceptID3= map_chr(CrossVariableConceptID3,paste,collapse = ", "),
             CrossVariableConceptID3_lookup= map_chr(CrossVariableConceptID3_lookup,paste,collapse = ", "),
             CrossVariableConceptValidValue3= map_chr(CrossVariableConceptValidValue3,paste,collapse = ", "),
             CrossVariableConceptValidValue3_lookup= map_chr(CrossVariableConceptValidValue3_lookup,paste,collapse = ", "),
             CrossVariableConceptID4= map_chr(CrossVariableConceptID4,paste,collapse = ", "),
             CrossVariableConceptID4_lookup= map_chr(CrossVariableConceptID4_lookup,paste,collapse = ", "),
             CrossVariableConceptValidValue4= map_chr(CrossVariableConceptValidValue4,paste,collapse = ", "),
             CrossVariableConceptValidValue4_lookup= map_chr(CrossVariableConceptValidValue4_lookup,paste,collapse = ", "),
) %>% writexl::write_xlsx(report_fid)

push_to_cloud_storage <- FALSE
if (push_to_cloud_storage) {
  # Authenticate with Google Storage and write report file to bucket
  scope <- c("https://www.googleapis.com/auth/cloud-platform")
  bucket <- "gs://qaqc_reports"
  token <- token_fetch(scopes=scope)
  gcs_auth(token=token)
  gcs_upload(report_fid, bucket=bucket, name="qc_report_recruitment")
}
# }