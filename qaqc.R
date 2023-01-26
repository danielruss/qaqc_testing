library(bigrquery)
library(tidyverse)
library(readxl)
library(rlang)

#  read dictionary from Github (TODO: Ask nicole about aggegate.json version of dictionary)
# Dictionary used by Daniel to develop code (from Lorena)
# dictionary <- rio::import("https://episphere.github.io/conceptGithubActions/aggregate.json",format = "json")

# Dictionary provided by Nicole on Jan 19, 2023:
#dictionary <- rio::import("https://github.com/episphere/conceptGithubActions/blob/master/aggregate.json",format = "json")
dictionary <- rio::import("https://raw.githubusercontent.com/episphere/conceptGithubActions/master/aggregate.json",format = "json")

#dl <- map(dictionary,"Variable Label") %>% compact()
dl <-  dictionary %>% map(~.x[["Variable Label"]] %||% .x[["Variable Name"]]) %>% compact()
dictionary_lookup<-function(x){
  if (is.list(x)){
    return( 
      x %>% map(~str_remove(.x,"^d_")) %>% 
        map(~str_remove(.x,"^state_d_")) %>% 
        map(~recode(.x,!!!dl,.default = "Not In Label Dictionary")) %>%
        map_chr(paste,collapse=", ")
    )
  }
  ## remove "d_" or "state_d_" that may be at the start of the Id.
  x <- str_remove(x,"^d_") %>% str_remove("^state_d_") %>% na_if("")
  recode(x,!!!dl,.default = "Not In Label Dictionary")
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

# I'm not really happy with this function...  I would like to pass
# in two vectors values and valid_values. This way we are not locked
# into 3 columns.  Note: NA %in% NA returns true, so if we are cross 
# validating 1 variable, the NAs in v2 and v2 dont effect the results.
crossValidate <- function(value1,valid_values1,value2,valid_values2,value3,valid_values3){
  (value1 %in% valid_values1) & (value2 %in% valid_values2) & (value3 %in% valid_values3)
}

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
  arg_list$CrossVariableConceptValidValue2 <- list(arg_list$CrossVariableConceptID1Value %||% "")
  
  arg_list$CrossVariableConceptID3 <- arg_list$CrossVariableConceptID3 %||% ""
  arg_list$CrossVariable3Value <- arg_list$CrossVariable3Value %||% ""
  arg_list$CrossVariableConceptValidValue3 <- list(arg_list$CrossVariableConceptID3Value %||% "")
  
  arg_list
}



prepare_report <- function(data,l,ids){
  data %>% 
    mutate(invalid_values=!!sym(l$ConceptID),
           value2="",
           date=l$date,
           ConceptID=l$ConceptID,
           ConceptID_value=dictionary_lookup(ConceptID),
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
           qc_test=l$Qctype
    )  %>%
    select(Connect_ID,
           token, 
           qc_test,
           ConceptID,
           ConceptID_value,
           date,
           ValidValues,
           ValidValues_lookup,
           invalid_values,
           CrossVariableConceptID1,
           CrossVariableConceptID1_lookup,
           CrossVariable1Value,
           CrossVariableConceptValidValue1,
           CrossVariableConceptValidValue1_lookup,
           CrossVariableConceptID2,
           CrossVariableConceptID2_lookup,
           CrossVariable2Value,
           CrossVariableConceptValidValue2,
           CrossVariableConceptValidValue2_lookup,
           CrossVariableConceptID3,
           CrossVariableConceptID3_lookup,
           CrossVariable3Value,
           CrossVariableConceptValidValue3,
           CrossVariableConceptValidValue3_lookup,
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
    },error=function(e){
      rules_error <- TRUE
      error_msg <- paste("Rules error: ",e) 
      warning("Caught error during crossvalidation.",
              "\n\tConcept_ID:",l$ConceptID,
              "\n\tXV_ID1:",l$CrossVariableConceptID1,
              "\n\tXV_ID2:",l$CrossVariableConceptID2,
              "\n\tXV_ID3:",l$CrossVariableConceptID3)
    })
    
    # JP NOTE: Everything in report rows passed the cross valid, so valid() will only 
    # run on the CIDs that passed the cross validation
    report_rows <- data %>% 
      ## add a column which is the value of the ConceptId of interest...
      mutate(value = cid_value,
             date=l$date,
             CrossVariable1Value=xv1_value,
             CrossVariable2Value=xv2_value,
             CrossVariable3Value=xv3_value) %>%
      filter(
        crossValidate(
          xv1_value,l$CrossVariableConceptID1Value,
          xv2_value,l$CrossVariableConceptID2Value,
          xv3_value,l$CrossVariableConceptID3Value)
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
#    qctype_na = is.na(Qctype),
    cid_not_in_data=ConceptID %!in% names(data),
    bad_xv1_cid = !CrossVariableConceptID1 %in|na% names(data), 
    bad_xv2_cid = !CrossVariableConceptID2 %in|na% names(data),
    bad_xv3_cid = !CrossVariableConceptID3 %in|na% names(data),
#    error = if_else(qctype_na,"The Qctype is NA.",""),
    error = if_else(cid_not_in_data,"CID not in BQ table",""),
    error = if_else(bad_xv1_cid, paste(error,"Cross Var. CID 1 not in BQ table"),error),
    error = if_else(bad_xv2_cid, paste(error,"Cross Var. CID 2 not in BQ table"),error),
    error = if_else(bad_xv3_cid, paste(error,"Cross Var. CID 3 not in BQ table"),error)
  )  %>% select(!cid_not_in_data:bad_xv3_cid)
}
report_bad_rules <- function(...){
  l=list(...)
  return(tibble(qc_test=l$Qctype,ConceptID=l$ConceptID,date=l$date,ValidValues=list(l$ValidValues),rules_error=l$error,
                CrossVariableConceptID1=l$CrossVariableConceptID1,CrossVariable1Value="",CrossVariableConceptValidValue1=list(l$CrossVariableConceptID1Value),
                CrossVariableConceptID2=l$CrossVariableConceptID2,CrossVariable2Value="",CrossVariableConceptValidValue2=list(l$CrossVariableConceptID2Value),
                CrossVariableConceptID3=l$CrossVariableConceptID3,CrossVariable3Value="",CrossVariableConceptValidValue3=list(l$CrossVariableConceptID3Value)
                ))
}

runQC <- function(data, rules, QC_report_location,ids){
  run_date=Sys.time()

  rules <- find_errors(rules,data)
  bad_rules <- rules %>% filter(nchar(error)>0) 
  filtered_rules <- rules %>% filter(!is.na(Qctype) & nchar(error)==0)
  rules <- filtered_rules %>% select(!error)
  

  # I assume you have a document that can be read into a tibble, a column can be something 
  # other than a simple primitive (e.g. a list/vector).  This is referred to as a list-column.
  # one column of the tibble is the QC Type (valid/crossvalid1).  You will want to split the 
  # table by QC_type...
  # check_valid <- rules_tibble %>% filter(QCType=="valid")
  # of course you can just stick this in the bind_rows command...
  
  bind_rows(
    bad_rules %>% pmap_dfr(report_bad_rules,date=run_date),
    rules %>% filter(Qctype=="valid") %>% pmap_dfr(valid,data=data,ids={{ids}},date=run_date),
    rules %>% filter(Qctype=="NA or valid") %>% pmap_dfr(na_or_valid,data=data,ids={{ids}},date=run_date),
    rules %>% filter(Qctype=="crossValid1") %>% pmap_dfr(crossvalid,data=data,ids={{ids}},date=run_date),
    rules %>% filter(Qctype=="crossValid2") %>% pmap_dfr(crossvalid,data=data,ids={{ids}},date=run_date),
    rules %>% filter(Qctype=="crossValid3") %>% pmap_dfr(crossvalid,data=data,ids={{ids}},date=run_date),
    rules %>% filter(Qctype=="NA or crossValid1") %>% pmap_dfr(na_or_crossvalid,data=data,ids={{ids}},date=run_date),
    rules %>% filter(Qctype=="NA or crossValid2") %>% pmap_dfr(na_or_crossvalid,data=data,ids={{ids}},date=run_date),
    rules %>% filter(Qctype=="NA or crossValid3") %>% pmap_dfr(na_or_crossvalid,data=data,ids={{ids}},date=run_date),
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
  project <- "nih-nci-dceg-connect-stg-5519" # just the project it gets billed from
  # sql <- "SELECT * FROM `nih-nci-dceg-connect-stg-5519.Connect.recruitment1` where Connect_ID is not NULL"
  # sql <- "SELECT * FROM `nih-nci-dceg-connect-stg-5519.FlatConnect.participants_JP` WHERE Connect_ID IS NOT NULL" # Daniel's 
  sql <- "SELECT * FROM `nih-nci-dceg-connect-stg-5519.FlatConnect.participants_JP`" # Jake's modification
  tb <- bq_project_query(project, sql)
  data <- bq_table_download(tb, bigint = c("character"))
}else{
  #data_file <- "~/test_data.json"
  data_file <- "data.json"
  data1 <- rio::import(data_file) %>% tibble::as_tibble()
}

## add a failure...
## invalid SITE
test <- data
test$d_827220437[[1]] <- "3"
## xvalid1 invalid
test$d_827220437[[2]] <- "4"
test$d_512820379[[2]] <- "854703046"


## I need to load the rules file....
#rules_file <- "QCRules_test2.xlsx"
rules_file <- "qc_rules_011823_na_corrected.xlsx"
#rules_file <- "qc_rules_011823.xlsx"
# rules <- read_excel(rules_file,col_types = 'text')
rules <- read_excel(rules_file,col_types = 'text') %>% 
  mutate(ValidValues=map(ValidValues,convertToVector),
         CrossVariableConceptID1Value=map(CrossVariableConceptID1Value,convertToVector),
         CrossVariableConceptID2Value=map(CrossVariableConceptID2Value,convertToVector),
         CrossVariableConceptID3Value=map(CrossVariableConceptID3Value,convertToVector) )

system.time(x <- runQC(data, rules,ids=Connect_ID))

col_order <- c("Connect_ID", "token", "qc_test", "rules_error", "ConceptID", "date",
               "ValidValues", "invalid_values",
               "CrossVariableConceptID1", "CrossVariable1Value", "CrossVariableConceptValidValue1",
               "CrossVariableConceptID2", "CrossVariable2Value", "CrossVariableConceptValidValue2",
               "CrossVariableConceptID3", "CrossVariable3Value", "CrossVariableConceptValidValue3",
               "ConceptID_value", "ValidValues_lookup",
               "CrossVariableConceptID1_lookup", "CrossVariableConceptValidValue1_lookup",
               "CrossVariableConceptID2_lookup", "CrossVariableConceptValidValue2_lookup",
               "CrossVariableConceptID3_lookup", "CrossVariableConceptValidValue3_lookup")
x <- x[, col_order]

x %>% mutate(ValidValues = map_chr(ValidValues,paste,collapse = ", "),
             CrossVariableConceptID1= map_chr(CrossVariableConceptID1,paste,collapse = ", "),
             CrossVariableConceptValidValue1= map_chr(CrossVariableConceptValidValue1,paste,collapse = ", "),
             CrossVariableConceptID2= map_chr(CrossVariableConceptID2,paste,collapse = ", "),
             CrossVariableConceptValidValue2= map_chr(CrossVariableConceptValidValue2,paste,collapse = ", "),
             CrossVariableConceptID3= map_chr(CrossVariableConceptID3,paste,collapse = ", "),
             CrossVariableConceptValidValue3= map_chr(CrossVariableConceptValidValue3,paste,collapse = ", "),
)  %>% writexl::write_xlsx("qc_out_011823_na_corrected.xlsx")