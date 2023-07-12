library(plumber)
library(sys)

#* heartbeat...
#* @get /
#* @post /
function() {
  return("alive")
}

#* Runs QAQC for recruitment data set
#* @param min_rule:int - row of rules file to start at
#* @param max_rule:int - row of rules file to end at
#* @get /qaqc-recruitment
#* @post /qaqc-recruitment
function(min_rule=1, max_rule=10000) {
  Sys.setenv(R_CONFIG_ACTIVE = "recruitment")
  Sys.setenv(MIN_RULE = min_rule)
  Sys.setenv(MAX_RULE = max_rule)
  message("Starting recruitment QAQC...")
  source("qaqc.R", echo = TRUE)
  return("Recruitment QAQC complete!")
}

#* Runs QAQC for biospecimen data set
#* @param min_rule:int - row of rules file to start at
#* @param max_rule:int - row of rules file to end at
#* @get /qaqc-biospecimen
#* @post /qaqc-biospecimen
function(min_rule=1, max_rule=10000) {
  message("Starting biospecimen QAQC...")
  Sys.setenv(R_CONFIG_ACTIVE = "biospecimen")
  Sys.setenv(MIN_RULE = min_rule)
  Sys.setenv(MAX_RULE = max_rule) 
  source("qaqc.R", echo = TRUE)
  return("Biospecimen QAQC complete!")
}

#* Runs QAQC for module 1 data set
#* @param min_rule:int - row of rules file to start at
#* @param max_rule:int - row of rules file to end at
#* @get /qaqc-module1
#* @post /qaqc-module1
function(min_rule=1, max_rule=10000) {
  message("Starting Module 1 QAQC...")
  Sys.setenv(R_CONFIG_ACTIVE = "module1")
  Sys.setenv(MIN_RULE = min_rule)
  Sys.setenv(MAX_RULE = max_rule)
  source("qaqc.R", echo = TRUE)
  return("Module 1 QAQC complete!")
}
