################################################################################
#############    Define Parameters Here IF Running Locally    ##################
################################################################################
local_drive <- "/Users/petersjm/Documents/qaqc_testing" #set to your working dir
tier        <- "prod" # "prod" or "stg"
module      <- "module1" # "recruitment", "biospecimen", "module1", "module2", "module3", or "module4"
testing_api <- FALSE # ONLY SET TO TRUE IF YOU ARE TESTING PLUMBER API
################################################################################
################################################################################

# Load Dependecies
library(plyr)
library(tidyverse)
library(bigrquery)
library(tidyverse)
library(rlang)
library(plumber)
library(googleCloudStorageR)
library(gargle)
library(readxl)
library(stringr)
library(glue)
library(janitor)
library(config)
library(writexl)


# Configure system variables for local run
if (local_drive == getwd() & testing_api == FALSE) {
  Sys.setenv(R_CONFIG_ACTIVE = module) # configuration to use
  Sys.setenv(R_CONFIG_FILE = glue("{tier}/config.yml")) # config path
  Sys.setenv(MIN_RULE = 0)     # rule to start at
  Sys.setenv(MAX_RULE = 10000) # rule to end at (pick large value to run all)
  Sys.setenv(START_INDEX = 0)
  Sys.setenv(N_MAX = Inf)
}

# Get parameters from configuration file
project       <- config::get("project_id")
billing       <- project
QC_REPORT     <- config::get("QC_REPORT")
rules_file    <- config::get("rules_file")
tier          <- config::get("tier")
bucket        <- config::get("bucket")
flag          <- config::get("flag")
boxfolder     <- config::get("box_folder")
write_to_gcs  <- if_else(local_drive == getwd(), FALSE, TRUE)
min_rule      <- Sys.getenv("MIN_RULE")
max_rule      <- Sys.getenv("MAX_RULE")
start_index   <- as.numeric(Sys.getenv("START_INDEX"))
n_max         <- as.numeric(Sys.getenv("N_MAX"))
sheet         <- NULL
exclusions_fid <- config::get("exclusions_file")

#Authenticate to bigrquery
bq_auth()

# Name of output/report file
rules_str  <- glue("rules{min_rule}to{max_rule}")
rows_str   <- glue("datarows{start_index}to{start_index+n_max}")
report_fid <-
  paste("qc_report", QC_REPORT, tier, flag, Sys.Date(), rules_str, rows_str, "boxfolder", boxfolder, ".xlsx", sep="_")

dictionary <- rio::import("https://episphere.github.io/conceptGithubActions/aggregate.json",format = "json")

# Fix the "No permanent teeth lost" issue with the Connnect data dictionary
dictionary$`104430631`$`Variable Label` <- "No"
dictionary$`104430631`$`Variable Name` <- "No"

dl <-  dictionary %>% map(~.x[["Variable Label"]] %||% .x[["Variable Name"]]) %>% compact()




######################### User-defined Functions ###############################


dictionary_lookup <- function(x){
  # This function performs a dictionary lookup on a list of values stored in a 
  # global variable named "dl". The dictionary keys are expected to have the 
  # format "d_CID", where CID is a unique identifier for each value. For each 
  # input value, the function retrieves the corresponding value from the dictionary.
  # If a value is not found in the dictionary, "NA" is used in the output list.
  x=as.list(x)
  skel <- as.relistable(x)
  x <- unlist(x)
  
  x <- x %>%
    str_split_i("d_", -1) %>% # Get last CID without "d_"
    map(~dl[[.x]]) %>%
    modify_if(is.null,~"NA")
  x <- relist(x,skel)
  class(x) <- "list"
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
`%!in%`   <- function(x,y){ !`%in%`(x,y) }
`%in|na%` <- function(x,y){ is.na(x) | x %in% y }

# I'm not really happy with this function...  I would like to pass
# in two vectors values and valid_values. This way we are not locked
# into 3 columns.  Note: NA %in% NA returns true, so if we are cross
# validating 1 variable, the NAs in v2 and v2 dont effect the results.
crossValidate <- function(value1,valid_values1,value2,valid_values2,value3,valid_values3,value4,valid_values4){
  (value1 %in% valid_values1) & (value2 %in% valid_values2) & (value3 %in% valid_values3) & (value4 %in% valid_values4)
}

# Takes a concept ID (CID) as a string and looks up its corresponding value in 
# the provided data tibble. If the CID is NA, it returns NA_character_ to 
# represent missing data.
getCIDValue <- function(cid,data){
  stopifnot(length(cid)==1)
  if (is.na(cid)) {
    return(NA_character_)
  }
  eval_tidy(sym(cid),data)
}

## Functions for initializing the report dataframe

# Initializes a list for report generation by ensuring all required variables 
# have defined values. If a variable is missing, it sets it to an empty string 
# or a default value. Used for creating a report dataframe.
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

# Generates a report dataframe by extracting specific columns from the input 
# data tibble and applying the necessary transformations based on provided rules.
prepare_report <- function(data,l,ids){
  data %>%
    mutate(rule_id=l$rule_id,
           site_id=data$d_827220437,
           site=dictionary_lookup(site_id),
           invalid_values=as.character(!!sym(l$ConceptID)),
           invalid_values_lookup=dictionary_lookup(!!sym(l$ConceptID)),
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
           rule_id,
           site_id,
           site,
           qc_test,
           rule_label,
           rule_error,
           ConceptID,
           ConceptID_lookup,
           date,
           ValidValues,
           ValidValues_lookup,
           invalid_values,
           invalid_values_lookup,
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
           #index,
           {{ids}})
}

# Adverb functions to be used to modify validation functions.
# Validates data based on cross-variable checks and filters report rows accordingly.
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

#Adverb function used to modify validation functions. Runs validation rules, 
#allowing NA values for the invalid_values column in the report.
na_ok <- function(f){
  function(data,ids,...){
    f(data,ids={{ids}},na_ok=TRUE,...) %>%
      filter(!is.na(invalid_values))
  }
}

# Applies validation rules to check data validity based on provided concept ID 
# and valid values. Filters report rows based on validation results.
valid <- function(data,ids,...){
  #message("... Running valid")
  # ok, this is a difficult concept, but the conceptId is passed in as string.
  # if you try to say "is the value of the conceptId that I passed in within a set of valid values
  # you would be testing if the string "d_xxxxxxx" is in the set of value values.  We need to let
  # R know that is a variable name (a symbol).  So we convert the conceptId to a symbol.  Then
  # we need to ask R if the value that the symbol points is in the set of valid values, not the symbol
  # itself.  So the !! dereferences the symbol to the value.
  l=list(...) # Column names that are passed in ...
  
  ## select all the invalid rows. and save it in the "failed" tibble...
  # failed if ConceptID is in ValidValues
  #       --> If valid values starts with "c(", evaluate it as r code
  #           i.e., "c(1:100)" becomes c(1,2,3,4,5,...,100)
  failed <- data %>%
    filter(!!sym(l$ConceptID) %!in% ifelse(
      startsWith(l$ValidValues, "c("),
      eval(parse(text = l$ValidValues)),
      l$ValidValues
    ))
  
  l <- prepare_list_for_report(l)
  
  ## quoted_conceptId = enquo(conceptId)
  ## for each failed row, get the Ids we want to place in the report...
  ## I also set the test to VALID
  ## you will need to set other values to as needed.
  
  prepare_report(failed,l,{{ids}})
}

# Applies cross-variable validation to report rows and filters based on validation results.
crossvalid <- crossvalidly(valid)

# Applies validation rules to check data validity, allowing NA values in the invalid_values column in the report.
na_or_valid <- na_ok(valid)

# Applies cross-variable validation, allowing NA values for the invalid_values column in the report.
na_or_crossvalid <- na_ok(crossvalid)

## The value of rules$Concept_ID must match that of rules$ValidValues [which is another CID]
match_cid_values <- function(data,ids,...){
  l=list(...) # Column names that are passed in ...
  # select all the invalid rows
  #print('made it to 308')
  failed <- data %>% filter( !!sym(l$ConceptID) != getCIDValue(l$ValidValues,data) )
  #print('made it to 310')
  l <- prepare_list_for_report(l)
  prepare_report(failed,l,{{ids}})
}
crossvalid_match_cid_values <- crossvalidly(match_cid_values)
na_or_match_cid_values <- na_ok(match_cid_values)
na_or_crossvalid_match_cid_values <- na_ok(crossvalid_match_cid_values)

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
  #l$ValidValues <- c(l$ValidValues, date_from_valid_vals)
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

# Filter values that are not NA
isNotPopulated <- function(data,ids,...){
  l=list(...) # Column names that are passed in ...
  
  value_to_check <- getCIDValue(l$ConceptID,data)
  failed <- data %>% filter(!is.na(value_to_check))
  
  l <- prepare_list_for_report(l)
  prepare_report(failed,l,{{ids}})
}
crossvalidIsNotPopulated <- crossvalidly(isPopulated)

# Filter values that cannot be converted to numeric
isNumeric <- function(data,ids,...){
  l=list(...) # Column names that are passed in ...
  
  # Try to convert value (can be string) to numeric, if as.numeric() returns NA, return false
  # is_numeric("5.5") == TRUE
  # is_numeric(5.5) == TRUE
  # is_numeric("five point five") == FALSE
  is_numeric <- function(x) !is.na(as.numeric(x))
  
  # select all the invalid rows
  failed <- data %>% filter(!is_numeric(!!sym(l$ConceptID)))
  l <- prepare_list_for_report(l)
  prepare_report(failed,l,{{ids}})
}
na_or_isNumeric <- na_ok(isNumeric)
crossvalid_isNumeric <- crossvalidly(isNumeric)
na_or_crossvalid_isNumeric <- na_ok(crossvalid_isNumeric)

# Filter values that are not valid dates
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
na_or_has_less_than_or_equal_n_characters <-na_ok(has_less_than_or_equal_n_characters)
crossvalid_has_less_than_or_equal_n_characters <- crossvalidly(has_less_than_or_equal_n_characters)
na_or_crossvalid_has_less_than_or_equal_n_characters <- na_ok(crossvalid_has_less_than_or_equal_n_characters)

# Check for valid time (0-23):(0-59)
is_24hr_time <-function(data,ids,...){
  l=list(...)
  valid_time <- function(x){grepl("^(?:[01]\\d|2[0-3]):([0-5]\\d)$",x)}
  failed <- data %>% filter(!valid_time(!!sym(l$ConceptID)))
  l <- prepare_list_for_report(l)
  prepare_report(failed,l,{{ids}})
}
na_or_is_24hr_time <- na_ok(is_24hr_time)
crossvalid_is_24hr_time <- crossvalidly(is_24hr_time)
na_or_crossvalid_is_24hr_time <- na_ok(crossvalid_is_24hr_time)


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
  return(tibble(rule_id=l$rule_id,
                qc_test=l$Qctype,
                ConceptID=l$ConceptID,
                date=l$date,
                ValidValues=list(l$ValidValues),
                rule_error=l$error,rule_label=l$Label,
                CrossVariableConceptID1=l$CrossVariableConceptID1,CrossVariable1Value="",CrossVariableConceptValidValue1=list(l$CrossVariableConceptID1Value),
                CrossVariableConceptID2=l$CrossVariableConceptID2,CrossVariable2Value="",CrossVariableConceptValidValue2=list(l$CrossVariableConceptID2Value),
                CrossVariableConceptID3=l$CrossVariableConceptID3,CrossVariable3Value="",CrossVariableConceptValidValue3=list(l$CrossVariableConceptID3Value),
                CrossVariableConceptID4=l$CrossVariableConceptID4,CrossVariable4Value="",CrossVariableConceptValidValue4=list(l$CrossVariableConceptID4Value),
                notes=l$`Notes from QC process`,
                rule_label=l$Label,
                .name_repair="universal"))
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
    rules %>% filter(Qctype=="valid before date()") %>% pmap_dfr(validBeforeDate,data=data,ids={{ids}},date=run_date),
    rules %>% filter(Qctype=="crossvalid before date()") %>% pmap_dfr(crossvalidBeforeDate,data=data,ids={{ids}},date=run_date),
    rules %>% filter(Qctype=="NA or valid before date()") %>% pmap_dfr(na_or_validBeforeDate,data=data,ids={{ids}},date=run_date),
    rules %>% filter(Qctype=="NA or crossvalid before date()") %>% pmap_dfr(na_or_crossvalidBeforeDate,data=data,ids={{ids}},date=run_date),
    rules %>% filter(Qctype=="NA or valid") %>% pmap_dfr(na_or_valid,data=data,ids={{ids}},date=run_date),
    rules %>% filter(Qctype=="is populated") %>% pmap_dfr(isPopulated,data=data,ids={{ids}},date=run_date),
    rules %>% filter(Qctype=="is not populated") %>% pmap_dfr(isNotPopulated,data=data,ids={{ids}},date=run_date),
    rules %>% filter(Qctype=="isNumeric") %>% pmap_dfr(isNumeric,data=data,ids={{ids}},date=run_date),
    rules %>% filter(Qctype=="crossValid1 isNumeric") %>% pmap_dfr(crossvalid_isNumeric,data=data,ids={{ids}},date=run_date),
    rules %>% filter(Qctype=="NA or isNumeric") %>% pmap_dfr(na_or_isNumeric,data=data,ids={{ids}},date=run_date),
    rules %>% filter(Qctype=="NA or crossValid1 isNumeric") %>% pmap_dfr(na_or_crossvalid_isNumeric,data=data,ids={{ids}},date=run_date),
    rules %>% filter(Qctype=="crossValid1") %>% pmap_dfr(crossvalid,data=data,ids={{ids}},date=run_date),
    rules %>% filter(Qctype=="crossValid2") %>% pmap_dfr(crossvalid,data=data,ids={{ids}},date=run_date),
    rules %>% filter(Qctype=="crossValid3") %>% pmap_dfr(crossvalid,data=data,ids={{ids}},date=run_date),
    rules %>% filter(Qctype=="crossValid4") %>% pmap_dfr(crossvalid,data=data,ids={{ids}},date=run_date),
    rules %>% filter(Qctype=="crossValid1 is populated") %>% pmap_dfr(crossvalidIsPopulated,data=data,ids={{ids}},date=run_date),
    rules %>% filter(Qctype=="crossValid1 is not populated") %>% pmap_dfr(crossvalidIsNotPopulated,data=data,ids={{ids}},date=run_date),
    rules %>% filter(Qctype=="NA or crossValid1") %>% pmap_dfr(na_or_crossvalid,data=data,ids={{ids}},date=run_date),
    rules %>% filter(Qctype=="NA or crossValid2") %>% pmap_dfr(na_or_crossvalid,data=data,ids={{ids}},date=run_date),
    rules %>% filter(Qctype=="NA or crossValid3") %>% pmap_dfr(na_or_crossvalid,data=data,ids={{ids}},date=run_date),
    rules %>% filter(Qctype=="NA or crossValid4") %>% pmap_dfr(na_or_crossvalid,data=data,ids={{ids}},date=run_date),
    rules %>% filter(Qctype=="crossValid1Date") %>% pmap_dfr(crossvalidDate,data=data,ids={{ids}},date=run_date),
    rules %>% filter(Qctype=="crossValid1NotNA") %>% pmap_dfr(crossValid_notNA,data=data,ids={{ids}},date=run_date),
    rules %>% filter(Qctype=="NA or dateTime") %>% pmap_dfr(na_or_datetime,data=data,ids={{ids}},date=run_date),
    rules %>% filter(Qctype=="NA or date") %>% pmap_dfr(na_or_date,data=data,ids={{ids}},date=run_date),
    rules %>% filter(Qctype=="crossValid1 equal to char()") %>% pmap_dfr(crossvalid_has_n_characters,data=data,ids={{ids}},date=run_date),
    rules %>% filter(Qctype=="NA or crossValid1 equal to char()") %>% pmap_dfr(crossvalid_has_n_characters,data=data,ids={{ids}},date=run_date),
    rules %>% filter(Qctype=="NA or equal to char()") %>% pmap_dfr(na_or_has_n_characters,data=data,ids={{ids}},date=run_date),
    rules %>% filter(Qctype=="crossValid1 equal to or less than char()") %>% pmap_dfr(crossvalid_has_less_than_or_equal_n_characters,data=data,ids={{ids}},date=run_date),
    rules %>% filter(Qctype=="NA or crossValid1 equal to or less than char()") %>% pmap_dfr(na_or_crossvalid_has_less_than_or_equal_n_characters,data=data,ids={{ids}},date=run_date),
    rules %>% filter(Qctype=="NA or equal to or less than char()") %>% pmap_dfr(na_or_has_less_than_or_equal_n_characters,data=data,ids={{ids}},date=run_date),
    rules %>% filter(Qctype=="match cid values") %>% pmap_dfr(match_cid_values,data=data,ids={{ids}},date=run_date),
    rules %>% filter(Qctype=="crossvalid match cid values") %>% pmap_dfr(crossvalid_match_cid_values,data=data,ids={{ids}},date=run_date),
    rules %>% filter(Qctype=="na or match cid values") %>% pmap_dfr(na_or_match_cid_values,data=data,ids={{ids}},date=run_date),
    rules %>% filter(Qctype=="na or crossvalid match cid values") %>% pmap_dfr(na_or_crossvalid_match_cid_values,data=data,ids={{ids}},date=run_date),
    rules %>% filter(Qctype=="is 24hr time") %>% pmap_dfr(is_24hr_time,data=data,ids={{ids}},date=run_date),
    rules %>% filter(Qctype=="NA or is 24hr time") %>% pmap_dfr(na_or_is_24hr_time,data=data,ids={{ids}},date=run_date),
    rules %>% filter(Qctype=="crossValid1 or is 24hr time") %>% pmap_dfr(crossvalid_is_24hr_time,data=data,ids={{ids}},date=run_date),
    rules %>% filter(Qctype=="NA or CrossValid1 is 24hr time") %>% pmap_dfr(na_or_crossvalid_is_24hr_time,data=data,ids={{ids}},date=run_date)
  )
}

loadData <- function(project, table, where_clause, 
                     start_index = start_index, 
                     n_max = n_max) {
  
    q <- sprintf("SELECT * FROM `%s.FlatConnect.%s` %s ORDER BY token", # DESC",
                 project, table, where_clause)
    
    if (n_max != Inf) {
      LIMIT <- sprintf("LIMIT %s", as.character(as.integer(n_max)))
    } else {
      LIMIT <- ""
    }
    
    if (start_index > 0) {
      OFFSET <- sprintf("OFFSET %s", as.character(as.integer(start_index)))
    } else {
      OFFSET <- ""
    }
    
    q <- glue("SELECT * 
               FROM `{project}.FlatConnect.{table}` 
               {where_clause} 
               ORDER BY token
               {LIMIT}
               {OFFSET}")
    print(q)
    tb   <- bq_project_query(project, query=q)
    data <- bq_table_download(tb, 
                              bigint="integer64", 
                              # page_size=1000, 
                              # start_index=start_index,
                              n_max=n_max,
                              quiet=TRUE)
    
    return(data)
}

get_explanation <- function(x, data) {
  x <- x %>%
    mutate(
      explanation = case_when(
        qc_test == "valid" ~
          (function(x)
            glue("[{x$ConceptID_lookup}] should be [{x$ValidValues_lookup}], but it's [{x$invalid_values_lookup}].")
          ) (x),
        
        qc_test == "NA or valid" ~
          (function(x)
            glue("[{x$ConceptID_lookup}] should be [{x$ValidValues_lookup}] or NA, but it's [{x$invalid_values_lookup}].")
          ) (x),
        
        qc_test == "crossValid1" ~
          (function(x)
            glue("If [{x$CrossVariableConceptID1_lookup}] is [{x$CrossVariableConceptValidValue1_lookup}], ",
                 "then [{x$ConceptID_lookup}] should be [{x$ValidValues_lookup}], but it's [{x$invalid_values_lookup}].")
          ) (x),
        
        qc_test == "crossValid2" ~
          (function(x)
            glue("If [{x$CrossVariableConceptID1_lookup}] is [{x$CrossVariableConceptValidValue1_lookup}]",
                 " & [{x$CrossVariableConceptID2_lookup}] is [{x$CrossVariableConceptValidValue2_lookup}], ",
                 "then [{x$ConceptID_lookup}], should be [{x$ValidValues_lookup}], but it's [{x$invalid_values_lookup}].")
          ) (x),
        
        qc_test == "crossValid3" ~
          (function(x)
            glue("If [{x$CrossVariableConceptID1_lookup}] is [{x$CrossVariableConceptValidValue1_lookup}],",
                 "[{x$CrossVariableConceptID2_lookup}] is [{x$CrossVariableConceptValidValue2_lookup}] ",
                 "& [{x$CrossVariableConceptID3_lookup}] is [{x$CrossVariableConceptValidValue3_lookup}], ",
                 "then [{x$ConceptID_lookup}] should be [{x$ValidValues_lookup}], but it's [{x$invalid_values_lookup}].")
          ) (x),
        
        qc_test == "crossValid4" ~
          (function(x)
            glue("If [{x$CrossVariableConceptID1_lookup}] is [{x$CrossVariableConceptValidValue1_lookup}], ",
                 "[{x$CrossVariableConceptID2_lookup}] is [{x$CrossVariableConceptValidValue2_lookup}], ",
                 "[{x$CrossVariableConceptID3_lookup}] is [{x$CrossVariableConceptValidValue3_lookup}] ",
                 "& [{x$CrossVariableConceptID4_lookup}] is [{x$CrossVariableConceptValidValue4_lookup}], ",
                 "then [{x$ConceptID_lookup}] should be [{x$ValidValues_lookup}], but it's [{x$invalid_values_lookup}].")
          ) (x),
        
        qc_test == "NA or crossValid1" ~
          (function(x)
            glue("If [{x$CrossVariableConceptID1_lookup}] is [{x$CrossVariableConceptValidValue1_lookup}] ",
                 "then [{x$ConceptID_lookup}] should be [{x$ValidValues_lookup}] or NA, but it's [{x$invalid_values_lookup}].")
          ) (x),
        
        qc_test == "NA or crossValid2" ~
          (function(x)
            glue("If [{x$CrossVariableConceptID1_lookup}] is [{x$CrossVariableConceptValidValue1_lookup}] ",
                 "& [{x$CrossVariableConceptID2_lookup}] is [{x$CrossVariableConceptValidValue2_lookup}], ",
                 "then [{x$ConceptID_lookup}] should be [{x$ValidValues_lookup}] or NA, but it's [{x$invalid_values_lookup}].")
          ) (x),
        
        qc_test == "NA or crossValid3" ~
          (function(x)
            glue("If [{x$CrossVariableConceptID1_lookup}] is [{x$CrossVariableConceptValidValue1_lookup}], ",
                 "[{x$CrossVariableConceptID2_lookup}] is [{x$CrossVariableConceptValidValue2_lookup}] ",
                 "& [{x$CrossVariableConceptID3_lookup}] is [{x$CrossVariableConceptValidValue3_lookup}], ",
                 "then [{x$ConceptID_lookup}] should be [{x$ValidValues_lookup}] or NA, but it's [{x$invalid_values_lookup}].")
          ) (x),
        
        qc_test == "NA or crossValid4" ~
          (function(x)
            glue("If [{x$CrossVariableConceptID1_lookup}] is [{x$CrossVariableConceptValidValue1_lookup}], ",
                 "[{x$CrossVariableConceptID2_lookup}] is [{x$CrossVariableConceptValidValue2_lookup}], ",
                 "[{x$CrossVariableConceptID3_lookup}] is [{x$CrossVariableConceptValidValue3_lookup}] ",
                 "& [{x$CrossVariableConceptID4_lookup}] is [{x$CrossVariableConceptValidValue4_lookup}], ",
                 "then [{x$ConceptID_lookup}] should be [{x$ValidValues_lookup}] or NA, but it's [{x$invalid_values_lookup}].")
          ) (x),
        
        qc_test == "is populated" ~ (function(x) glue("[{x$ConceptID_lookup}] should be populated, but it's missing.")) (x),
        
        qc_test == "crossValid1 equal to char()" ~
          (function(x)
            glue("If [{x$CrossVariableConceptID1_lookup}] is [{x$CrossVariableConceptValidValue1_lookup}], ",
                 "then [{x$ConceptID_lookup}] should be a string of length [{x$ValidValues}], ",
                 "but it's [{x$invalid_values}] with length [{length(x$invalid_values)}].")
          ) (x),
        
        qc_test == "NA or equal to char()" ~
          (function(x)
            glue("[{x$ConceptID_lookup}] should either be NA or a string of length [{x$ValidValues}], ",
                 "but it's [{x$invalid_values}] with length [{length(x$invalid_values)}].")
          ) (x),
        
        qc_test == "crossValid1 equal to or less than char()" ~
          (function(x)
            glue("If [{x$CrossVariableConceptID1_lookup}] is [{x$CrossVariableConceptValidValue1_lookup}], ",
                 "then [{x$ConceptID_lookup}] should be a string of length [{x$ValidValues}] or less, ",
                 "but it's [{x$invalid_values}] with length [{length(x$invalid_values)}].")
          ) (x),
        
        qc_test == "NA or equal to or less than char()" ~
          (function(x)
            glue("[{x$ConceptID_lookup}] should be NA or a string of length [{x$ValidValues}] or less, ",
                 "but it's [{x$invalid_values}] with length [{length(x$invalid_values)}].")
          ) (x),
        
        qc_test == "crossValid1Date" ~
          (function(x)
            glue("If [{x$CrossVariableConceptID1_lookup}] is [{x$CrossVariableConceptValidValue1_lookup}], ",
                 "then [{x$ConceptID_lookup}] should be a valid date, but it's [{x$invalid_values}]")
          ) (x),
        
        qc_test == "crossValid1NotNA" ~
          (function(x)
            glue("If [{x$CrossVariableConceptID1_lookup}] is [{x$CrossVariableConceptValidValue1_lookup}], ",
                 "then [{x$ConceptID_lookup}] must not be NA, but it's [{x$invalid_values_lookup}]")
          ) (x),
        
        qc_test == "NA or crossvalid before date()" ~
          (function(x)
            glue("If [{x$CrossVariableConceptID1_lookup}] is [{x$CrossVariableConceptValidValue1_lookup}], ",
                 "then [{x$ConceptID_lookup}] should be before [{x$ValidValues_lookup}] or NA, ",
                 "but they are [{x$invalid_values}] and [], respectively.")
          ) (x),
        
        qc_test == "NA or valid before date()" ~
          (function(x)
            glue("[{x$ConceptID_lookup}] should be before [{x$ValidValues_lookup}] or NA, ",
                 "but they are [{x$invalid_values}] and [], respectively.")
          ) (x),
        
        qc_test == "valid before date()" ~
          (function(x)
            glue("[{x$ConceptID_lookup}] should be before [{x$ValidValues}], ",
                 "but they are [{x$invalid_values}] and [], respectively.")
          ) (x),
        
        qc_test == "crossvalid match cid values" ~
          (function(x, data)
            glue("NA")
           # print(paste0("indices: ", x$index), ",/n", "value: ",
           #       getCIDValue(x$ConceptID, data[where(data$token==x$token)]))
           # print(paste0("x$ConceptID: ", getCIDValue(x$ConceptID,data), ", x$ValidValues: ", x$ConceptID))
           # glue("If [{x$CrossVariableConceptID1_lookup}] is [{x$CrossVariableConceptValidValue1_lookup}] ",
           #      "then [{x$ConceptID_lookup}] should have the same value as [{x$ValidValues_lookup}] or NA, ",
           #      "but [{x$ConceptID_lookup}] is [{getCIDValue(x$ConceptID, data)}] and [{x$ValidValues_lookup}] is [{getCIDValue(x$ValidValues, data)}].")
          ) (x, data),
        .default = "NA"
        
      )
    )
}


#################### Start of Main Script ######################################
#### YOU WILL WANT TO CHANGE THIS TO TRUE...
loadFromBQ <- TRUE
if (loadFromBQ){
  
  if (QC_REPORT == "biospecimen") {
    source("get_merged_biospecimen_and_recruitment_data.R")
    data <- get_merged_biospecimen_and_recruitment_data(project, 
                                                        exclude_duplicates=TRUE)
  }
  else if (QC_REPORT == "recruitment") {
    tables              <- c('participants_JP')
    where_clause        <- "WHERE d_831041022='104430631'" # AND TIMESTAMP(d_914594314) >= TIMESTAMP('2023-12-01T00:00:00.000Z')"
    data <- loadData(project, 
                     tables, 
                     where_clause, 
                     start_index=start_index, 
                     n_max=n_max)
  }
  else if (QC_REPORT == "module1") {
    source("get_merged_module_1_data.R")
    data <- get_merged_module_1_data(project)

# -------------------------------------------------------------------------

    
    # Deal with exceptions 
    source("check_and_correct_exceptions.R")
    exceptions <- config::get(value="exceptions", config = "module1")
    exceptions <- union(names(data), exceptions) # get only the ones that are actually in the df
    data <- check_and_correct_exceptions(data, exceptions)
  }
  else if (QC_REPORT == "module2") {
    source("get_merged_module_2_data.R")
    data <- get_merged_module_2_data(project)
    
    # Deal with exceptions
    source("check_and_correct_exceptions.R")
    exceptions <- config::get(value="exceptions", config = "module2")
    exceptions <- union(names(data), exceptions) # get only the ones that are actually in the df
    data <- check_and_correct_exceptions(data, exceptions)
  }
  else if (QC_REPORT == "module3") {
    source("get_merged_module_3_data.R")
    data <- get_merged_module_3_data(project)
    
    # Deal with exceptions 
    source("check_and_correct_exceptions.R")
    exceptions <- config::get(value="exceptions", config = "module3")
    exceptions <- union(names(data), exceptions) # get only the ones that are actually in the df
    data <- check_and_correct_exceptions(data, exceptions)
  }
  else if (QC_REPORT == "module4") {
    source("get_merged_module_4_data.R")
    data <- get_merged_module_4_data(project)
    
    # Deal with exceptions 
    source("check_and_correct_exceptions.R")
    exceptions <- config::get(value="exceptions", config = "module4")
    exceptions <- union(names(data), exceptions) # get only the ones that are actually in the df
    data <- check_and_correct_exceptions(data, exceptions)
  }
  # Add a row of indices so that we can refer back to the original position later
  # after filtering
  # data$index <- 1:nrow(data)
  
}else{
  data_file <- "data.json"
  data      <- rio::import(data_file) %>% tibble::as_tibble()
}

## I need to load the rules file....
# First get just the column names from the rules file
rules <-
  read_excel(
    rules_file,
    sheet = sheet,
    col_types = 'text'
  ) %>%
  mutate(
    ValidValues = map(ValidValues, convertToVector),
    CrossVariableConceptID1Value = map(CrossVariableConceptID1Value, convertToVector),
    CrossVariableConceptID2Value = map(CrossVariableConceptID2Value, convertToVector),
    CrossVariableConceptID3Value = map(CrossVariableConceptID3Value, convertToVector),
    CrossVariableConceptID4Value = map(CrossVariableConceptID4Value, convertToVector)
  )

# Make sure the specified last rule is not greater than the number of rules available
max_rule <- ifelse(max_rule >= nrow(rules), max_rule, nrow(rules))

# Keep just the specified rules
rules <- rules[min_rule:max_rule,] 

# print( system.time(x <- runQC(data, rules,ids=Connect_ID)) )
# Run QC report
Rprof()
x <- runQC(data, rules, ids=Connect_ID)

if (length(x)==0) {
  print("No qc issues found. No report generated.")
} else {
  # Convert arrays to strings so that they can be written to excel correctly,
  # otherwise arrays of strings appear as blank cells in excel sheet
  x <- x %>% mutate(
    site = map_chr(site,paste,collapse=", "),
    ConceptID_lookup = map_chr(ConceptID_lookup,paste,collapse=", "),
    ValidValues = map_chr(ValidValues,paste,collapse = ", "),
    ValidValues_lookup = map_chr(ValidValues_lookup,paste,collapse = ", "),
    invalid_values_lookup = map_chr(invalid_values_lookup,paste,collapse = ", "),
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
  ) %>%
    get_explanation()

  
  # Alter column order
  col_order <- c("Connect_ID", "token", "rule_id",
                 "site_id", "site",
                 "qc_test", "rule_error", "rule_label", "ConceptID", "ConceptID_lookup",
                 "ValidValues", "ValidValues_lookup", "invalid_values",
                 "CrossVariableConceptID1", "CrossVariableConceptID1_lookup", "CrossVariableConceptValidValue1", "CrossVariableConceptValidValue1_lookup",
                 "CrossVariableConceptID2", "CrossVariableConceptID2_lookup", "CrossVariableConceptValidValue2", "CrossVariableConceptValidValue2_lookup",
                 "CrossVariableConceptID3", "CrossVariableConceptID3_lookup", "CrossVariableConceptValidValue3", "CrossVariableConceptValidValue3_lookup",
                 "CrossVariableConceptID4", "CrossVariableConceptID4_lookup", "CrossVariableConceptValidValue4", "CrossVariableConceptValidValue4_lookup",
                 "date", "explanation")
  x <- x[, col_order] 
  
  if (!is.null(exclusions_fid) && exclusions_fid != "") {
    # Read in exclusions file
    exclusions <- read.csv(exclusions_fid)
    # Remove exclusions from report
    
    idx_exclusions <- which(x$rule_id %in% exclusions$rule_id & 
                            x$token %in% exclusions$token)
    report_df      <- x[-idx_exclusions,]
    report_df$toIgnore <- rep("FALSE", nrow(report_df))

    exclusions_df  <- x[idx_exclusions,]
    exclusions_df$toIgnore <- rep("TRUE", nrow(exclusions_df))
    
    # Write report and rules to separate sheets of excel file
    writexl::write_xlsx(list(report=report_df, exclusions=exclusions_df, rules=rules), report_fid)
  } else {
    # Write report and rules to separate sheets of excel file
    x$toIgnore <- rep("FALSE", nrow(x))
    writexl::write_xlsx(list(report=x, rules=rules), report_fid)
  }
  
  print(glue("{report_fid} save to local drive."))

  
  # Upload report to cloud storage if desired
  if (write_to_gcs) {
    print(glue("Uploading {report_fid} to {bucket}."))
    # Authenticate with Google Storage and write report file to bucket
    scope  <- c("https://www.googleapis.com/auth/cloud-platform")
    token  <- token_fetch(scopes=scope)
    gcs_auth(token=token)
    gcs_upload(report_fid, bucket=bucket, name=report_fid)
    print("Successfully uploaded to GCS Bucket.")
  }
}
Rprof(NULL)

