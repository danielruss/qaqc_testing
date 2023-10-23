#' Check and Correct Exceptional Columns
#'
#' This function checks for exceptional columns in a dataframe and applies corrections based on specified criteria.
# An exceptional column is identified based on the following criteria:
# 1. It contains at most three unique values (excluding NA).
# 2. All unique values are included in the specified valid values.
# 3. It contains at most one unique concept ID enclosed in square brackets "[...]".
#
#' If a column is identified as exceptional, this function performs the following corrections:
# - Replaces "[]" with NA.
# - Removes square brackets from concept IDs like "[conceptID]".

#' @param df A dataframe to check and correct exceptional columns.
#' @param cols_to_check A character vector specifying the names of columns to check and correct.
#' @param valid_values A character vector of valid values, including those enclosed in square brackets. Default is c(NA, "[]", "[178420302]", "[958239616]").
#'
#' @return A modified dataframe with exceptional columns corrected.
#'
#' @examples
#' # Create a sample dataframe
#' df <- data.frame(
#'   col1 = c(NA, "[]", "[178420302]", "[178420302]", "[178420302]", NA),
#'   col2 = c("[]", "[]", "invalid", "[987654321]", "[327986541]", "[]"),
#'   col3 = c("[123456789]", "[123456789]", "[123456789]", "[987654321]", "[327986541]", "[123456789]"),
#'   col4 = c("[]", NA, "[958239616]", NA, "[958239616]", "[958239616]"),
#'   col5 = c("[]", NA, "[958239616]", NA, "[178420302]", "[958239616]")
#' )
#'
#' # Define the columns to check and correct
#' columns_to_check <- c("col1", "col2", "col3")
#'
#' # Run the check_and_correct_exceptions function
#' df_processed <- check_and_correct_exceptions(df, columns_to_check)
#'
#' @export
check_and_correct_exceptions <- function(df, cols_to_check, valid_values = c(NA, "[]", "[178420302]", "[958239616]")) {
  df_copy <- df  # Create a copy of the original dataframe
  
  for (col_name in cols_to_check) {
    if (col_name %in% names(df) && is_exceptional_column(df[[col_name]], valid_values)) {
      # Step 1: Convert "[]" to NA
      df_copy[[col_name]][df_copy[[col_name]] == "[]"] <- NA
      # Step 2: Remove brackets, "[xxxxxxxxx]" to "xxxxxxxxx"
      df_copy[[col_name]] <- gsub("\\[(\\d{9})\\]", "\\1", df_copy[[col_name]])
    }
  }
  
  return(df_copy)
}



#' Check if a column is exceptional
#'
#' This function checks if a column is exceptional based on the specified valid values.
# An exceptional column is defined by the following criteria:
# 1. It contains at most three unique values (excluding NA).
# 2. All unique values are included in the specified valid values.
# 3. It contains at most one unique concept ID enclosed in square brackets "[...]".

#' @param x A vector to check for exceptional values.
#' @param valid_values A character vector of valid values, including those enclosed in square brackets.
#'
#' @return TRUE if the column is exceptional, FALSE otherwise.
#' @seealso check_and_correct_exceptions
is_exceptional_column <- function(x, valid_values) {
  unique_values <- unique(x)
  num_unique_values <- length(unique_values[!is.na(unique_values)])
  
  # Check if there are at most 3 unique values (excluding NA) and at most one unique concept ID
  check <- num_unique_values <= 3 && all(unique_values %in% valid_values) &&
    (sum(grepl("\\[\\d{9}\\]", unique_values)) <= 1)
  
  return(check)
}


## Uncomment this for testing
# # Create a sample dataframe
# df <- data.frame(
#   col1 = c(NA, "[]", "[178420302]", "[178420302]", "[178420302]", NA),
#   col2 = c("[]", "[]", "invalid", "[987654321]", "[327986541]", "[]"),
#   col3 = c("[123456789]", "[123456789]", "[123456789]", "[987654321]", "[327986541]", "[123456789]"),
#   col4 = c("[]", NA, "[958239616]", NA, "[958239616]", "[958239616]"),
#   col5 = c("[]", NA, "[958239616]", NA, "[178420302]", "[958239616]")
# )
# 
# cols_to_check <- c('col1', 'col2', 'col3', 'col4', 'col5')
# df_out <- check_and_correct_exceptions(df, cols_to_check)

# df_out <- data.frame(
#   col1 = c(NA, NA, "178420302", "178420302", "178420302", NA),
#   col2 = c("[]", "[]", "invalid", "[987654321]", "[327986541]", "[]"),
#   col3 = c("[123456789]", "[123456789]", "[123456789]", "[987654321]", "[327986541]", "[123456789]"),
#   col4 = c(NA, NA, "958239616", NA, "958239616", "958239616"),
#   col5 = c("[]", NA, "[958239616]", NA, "[178420302]", "[958239616]"),
# )
