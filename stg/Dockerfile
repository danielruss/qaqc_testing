FROM rocker/tidyverse:latest
RUN install2.r rio plumber bigrquery googleCloudStorageR gargle janitor rlang config
COPY ["../qaqc.R", "./qaqc.R"]
COPY ["../api.R", "./api.R"]
COPY ["./config.yml", "./config.yml"]
ENTRYPOINT ["R", "-e", "pr <- plumber::plumb('api.R'); pr$run(host='0.0.0.0', port=as.numeric(Sys.getenv('PORT')))"]