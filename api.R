library(plumber)
library(sys)

#* heartbeat...
#* @get /
#* @post /
function() {
  return("alive")
}

#* Run all qaqc reports
#* @get /qaqc-all
#* @post /qaqc-all
function() {

  echo = TRUE # Set this to TRUE when debugging
  message("Running QAQC...")

  # Set config for recruitment and run QAQC
  message("Starting recruitment QAQC...")
  Sys.setenv(R_CONFIG_ACTIVE = "recruitment")
  source("qaqc.R", echo = echo)
  message("Finished recruitment QAQC!")

  # Set config for biospecimen and run QAQC
  message("Starting biospecimen QAQC...")
  Sys.setenv(R_CONFIG_ACTIVE = "biospecimen")
  source("qaqc.R", echo = echo)
  message("Finished biospecimen QAQC!")

  # Set config for module 1 and run QAQC
  message("Starting Module 1 QAQC...")
  Sys.setenv(R_CONFIG_ACTIVE = "module1")
  source("qaqc.R", echo = echo)
  message("Finished Module 1 QAQC!")

  return("All QAQC reports complete!")
}

#* Runs QAQC for recruitment data set
#* @param min_rule:int - row of rules file to start at
#* @param max_rule:int - row of rules file to end at
#* @get /qaqc-recruitment
#* @post /qaqc-recruitment
function(min_rule=NULL, max_rule=NULL) {
  Sys.setenv(R_CONFIG_ACTIVE = "recruitment")
  Sys.setenv(MIN_RULE = min_rule)
  Sys.setenv(MAX_RULE = max_rule)
  message("Starting recruitment QAQC...")
  source("qaqc.R", echo = TRUE)
  return("Recruitment QAQC complete!")
}

#* Runs QAQC for biospecimen data set
#* @get /qaqc-biospecimen
#* @post /qaqc-biospecimen
function() {
  message("Starting biospecimen QAQC...")
  Sys.setenv(R_CONFIG_ACTIVE = "biospecimen")
  source("qaqc.R", echo = TRUE)
  return("Biospecimen QAQC complete!")
}

#* Runs QAQC for module 1 data set
#* @get /qaqc-module1
#* @post /qaqc-module1
function() {
  message("Starting Module 1 QAQC...")
  Sys.setenv(R_CONFIG_ACTIVE = "module1")
  source("qaqc.R", echo = TRUE)
  return("Module 1 QAQC complete!")
}
