FROM rocker/tidyverse:latest
RUN install2.r --error plumber rlang readxl rlang
COPY ["./qaqc.R", "./qaqc.R"]
COPY ["./qc_rules.xlsx", "./qc_rules.xlsx"]
ENTRYPOINT ["R", "-e", "pr <- plumber::plumb('qaqc.r'); pr$run(host='0.0.0.0', port=as.numeric(Sys.getenv('PORT')))"]