library(plumber)
library(sys)

#* heartbeat...
#* @get /
#* @post /
function() {
  return("alive")
}

#* Runs QAQC for recruitment data set
#* @get /qaqc-recruitment
#* @post /qaqc-recruitment
function() {
  Sys.setenv(R_CONFIG_ACTIVE = "recruitment")
  source("qaqc.R", echo = TRUE)
}

#* Runs QAQC for biospecimen data set
#* @get /qaqc-biospecimen
#* @post /qaqc-biospecimen
function() {
  Sys.setenv(R_CONFIG_ACTIVE = "biospecimen")
  source("qaqc.R", echo = TRUE)
}

#* Runs QAQC for module 1 data set
#* @get /qaqc-module1
#* @post /qaqc-module1
function() {
  Sys.setenv(R_CONFIG_ACTIVE = "module1")
  source("qaqc.R", echo = TRUE)
}