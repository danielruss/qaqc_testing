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
  
  echo = FALSE # Set this to true when debugging
  Print("Running QAQC...")
  
  # Set config for recruitment and run QAQC
  Sys.setenv(R_CONFIG_ACTIVE = "recruitment")
  source("qaqc.R", echo = echo)
  
  # Set config for biospecimen and run QAQC
  Sys.setenv(R_CONFIG_ACTIVE = "biospecimen")
  source("qaqc.R", echo = echo)

  # Set config for module 1 and run QAQC
  Sys.setenv(R_CONFIG_ACTIVE = "module1")
  source("qaqc.R", echo = echo)
  
  return("All QAQC reports complete!")
}

#* Runs QAQC for recruitment data set
#* @get /qaqc-recruitment
#* @post /qaqc-recruitment
function() {
  Sys.setenv(R_CONFIG_ACTIVE = "recruitment")
  source("qaqc.R", echo = TRUE)
  return("Recruitment QAQC complete!")
}

#* Runs QAQC for biospecimen data set
#* @get /qaqc-biospecimen
#* @post /qaqc-biospecimen
function() {
  Sys.setenv(R_CONFIG_ACTIVE = "biospecimen")
  source("qaqc.R", echo = TRUE)
  return("Biospecimen QAQC complete!")
}

#* Runs QAQC for module 1 data set
#* @get /qaqc-module1
#* @post /qaqc-module1
function() {
  Sys.setenv(R_CONFIG_ACTIVE = "module1")
  source("qaqc.R", echo = TRUE)
  return("Module 1 QAQC complete!")
}