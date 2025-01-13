# Load required libraries
library(bigrquery)
library(glue)
library(parallel)  # For mclapply

# Authenticate with BigQuery
bq_auth()

# Set report parameters
tier       <- "prod"
dataset    <- "recruitment"
dataset_id <- "FlatConnect"
table      <- "participants_JP"

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
total_rows   <- as.numeric(tbl_metadata$numRows)
cat("Number of Rows:", total_rows, "\n")

# Define number of chunks to use
rows_per_chunk <- 25000
num_chunks <- ceiling(total_rows / rows_per_chunk)
cat("Number of Chunks:", num_chunks, "\n")

# Directory where completed files are stored (adjust if needed)
output_dir <- "results"

# Define a pattern matching your output file naming scheme.
# Example pattern: qc_report_recruitment_prod_2025-01-13_chunk_14_boxfolder_211674408263_.xlsx
pattern <- "^qc_report_recruitment_.*_chunk_(\\d+)_.*\\.xlsx$"

# List existing output files in the output directory
completed_files <- list.files(path = output_dir, pattern = pattern, full.names = FALSE)
if(length(completed_files) > 0) {
  # Extract chunk numbers from filenames using a capturing regex
  # The sub() call replaces the full filename with just the chunk number.
  completed_chunks <- as.numeric(sub(pattern, "\\1", completed_files))
  cat("Completed chunks:", paste(completed_chunks, collapse = ", "), "\n")
} else {
  completed_chunks <- numeric(0)
  cat("No previously completed chunks found.\n")
}

# Calculate the list of chunks to run
all_chunks <- 0:(num_chunks - 1)
chunks_to_run <- setdiff(all_chunks, completed_chunks)
cat("Chunks to process:", paste(chunks_to_run, collapse = ", "), "\n")

# Define a retry function for downloading a chunk with error handling
download_chunk <- function(chunk_job, chunk_index, max_retries = 3) {
  attempts <- 0
  result <- NULL

  while (attempts < max_retries) {
    attempts <- attempts + 1
    result <- tryCatch({
      bq_table_download(chunk_job)
    }, error = function(e) {
      message(glue("Error downloading chunk {chunk_index}, attempt {attempts}: {e$message}"))
      NULL
    })

    if (!is.null(result)) {
      return(result)
    }

    Sys.sleep(1)  # Wait before retrying
  }

  stop(glue("Failed to download chunk {chunk_index} after {max_retries} attempts."))
}

# Define the run_qaqc function (ensure that qaqc.R exists and is configured properly)
run_qaqc <- function(dataset,
                     data = NULL,
                     chunk_num = NULL,
                     min_rule = 0,
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
  message(glue("Starting {dataset} QAQC for chunk {chunk_num}..."))

  # Create a list of parameters and put them in a new environment
  params <- list(data = data, chunk_num = chunk_num)
  params_env <- new.env()
  list2env(params, envir = params_env)

  # Source the QAQC script in the custom environment
  source("qaqc.R", local = params_env, echo = TRUE)

  return(glue("{dataset} QAQC complete for chunk {chunk_num}!"))
}

# Define a function that processes each chunk
process_chunk <- function(i) {
  # Build the query for the i-th chunk
  query <- glue("
    SELECT *
    FROM `{project_id}.{dataset_id}.{table}`
    WHERE MOD(FARM_FINGERPRINT(token), {num_chunks}) = {i}
  ")

  # Run the query and get the job object
  chunk_job <- bq_project_query(project_id, query)

  # Download the chunk with our retry mechanism
  data_chunk <- download_chunk(chunk_job, i)

  # (Optional) Make sure the output directory exists
  if (!dir.exists(output_dir)) {
    dir.create(output_dir)
  }

  # Optionally, write the chunk to a CSV if needed
  tmp_file <- glue("{output_dir}/{dataset_id}_chunk_{i}.csv")
  # write_csv(data_chunk, tmp_file)

  # Run QAQC on the chunk
  result <- run_qaqc(dataset, data = data_chunk, chunk_num = i)

  # Return the result (for logging or further processing)
  return(result)
}

# Run the process in parallel using mclapply on only the missing chunks.
# Here, we've reduced cores to 4 to help with network stability.
if (length(chunks_to_run) > 0) {
  results <- mclapply(chunks_to_run, process_chunk, mc.cores = 4)
  print(results)
} else {
  message("No new chunks to process.")
}
