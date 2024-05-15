#* This script downloads the data in chunks and runs QAQC on each chunk.

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

# -------------------------------------------------------------------------


run_qaqc <- function(dataset,
         min_rule = 1,
         max_rule = 10000,
         start_index = 0,
         n_max = Inf) {
  # Set environment variables to be used in the QAQC process
  Sys.setenv(R_CONFIG_FILE = glue("{tier}/config.yml"))
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

############# Main Script #############
# Set report parameters
tier       <- "prod"
dataset    <- "recruitment"
dataset_id <- "FlatConnect"
table      <- "participants_JP"

# Load libraries 
library(bigrquery)
library(glue)
bq_auth()

# Use switch to select the project based on the tier
project_id <- switch(tier,
                     prod = "nih-nci-dceg-connect-prod-6d04",
                     stg  = "nih-nci-dceg-connect-stg-5519",
                     dev  = "nih-nci-dceg-connect-dev",
                     "unknown-environment")

Sys.setenv(R_CONFIG_FILE = glue("{tier}/config.yml"))
tbl <- bq_table(project_id, dataset_id, table)

# Extract the row count from the metadata
tbl_metadata <- bq_table_meta(tbl)
total_rows   <- tbl_metadata$numRows
cat("Number of Rows:", total_rows, "\n")

# Set parameters for downloading in chunks
interval <- 100000  # Adjust as needed

# Loop through and download rows in chunks
for (start_index in seq(0, total_rows, interval)) {

  print(glue("start_index: {start_index} last_row: {start_index + interval - 1}"))
  
  run_qaqc(dataset, 
           start_index = as.character(as.integer(start_index)),
           n_max = as.character(as.integer(interval-1)),
           min_rule = 0)
}

