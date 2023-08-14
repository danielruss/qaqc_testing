#' Plumber API for Quality Assurance and Control
#' 
#' Author: Jake Peters
#' Date: Fall 2022
#' Last Updated: Aug. 2023
#' 
#' This Plumber API provides endpoints for running QAQC on different datasets.

library(plumber)
library(sys)
library(glue)

#* Heartbeat endpoint
#*
#* This endpoint checks if the API is alive.
#* 
#* @get /
#* @post /
function() {
  return("alive")
}

#* Runs QAQC for a specified dataset
#*
#* This endpoint runs Quality Assurance and Control on a specific dataset.
#*
#* @param dataset:str Character string indicating the dataset to be processed. Possible values are "recruitment", "biospecimen", "module1", or "module2".
#*
#* @param min_rule:int Integer indicating the row number of the rules file to start at.
#*
#* @param max_rule:int Integer indicating the row number of the rules file to end at.
#*
#* @param start_index:int Integer indicating the row number of the BQ table to start at.
#*
#* @param n_max:str Value indicating the maximum number of BQ table rows to read in. Note that this must be a string rather than a number to allow for Inf as a default value.
#*
#* @get /run-qaqc
#* @post /run-qaqc
function(dataset,
         min_rule = 1,
         max_rule = 10000,
         start_index = 0,
         n_max = Inf) {
  # Set environment variables to be used in the QAQC process
  Sys.setenv(R_CONFIG_ACTIVE = dataset) # Determines which config from config.yml to use
  Sys.setenv(MIN_RULE = as.integer(min_rule))
  Sys.setenv(MAX_RULE = as.integer(max_rule))
  Sys.setenv(START_INDEX = as.integer(start_index))
  Sys.setenv(N_MAX = as.double(n_max))
  
  # Inform the user that the QAQC process is starting
  message(glue("Starting {dataset} QAQC..."))
  
  # Source the QAQC script to run the quality checks
  source("qaqc.R", echo = TRUE)
  
  # Return a message indicating the completion of the QAQC process
  return(glue("{dataset} QAQC complete!"))
}
