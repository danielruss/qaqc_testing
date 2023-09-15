FROM rocker/tidyverse:latest
RUN install2.r plyr rio plumber bigrquery googleCloudStorageR gargle janitor rlang config writexl
COPY ["./qaqc.R", "./qaqc.R"]
COPY ["./api.R",  "./api.R"]
COPY ["./stg/config.yml", "./config.yml"] # copy config.yml from local stg directory to main directory in cloud run
COPY ["./get_merged_module_1_data.R", "./get_merged_module_1_data.R"]
COPY ["./get_merged_module_2_data.R", "./get_merged_module_2_data.R"]
COPY ["./get_merged_biospecimen_and_recruitment_data.R", "./get_merged_biospecimen_and_recruitment_data.R"]
COPY ["./qc_rules_recruitment.xlsx", "./qc_rules_recruitment.xlsx"]
COPY ["./qc_rules_biospecimen.xlsx", "./qc_rules_biospecimen.xlsx"]
COPY ["./qc_rules_module1.xlsx", "./qc_rules_module1.xlsx"]
ENTRYPOINT ["R", "-e", "pr <- plumber::plumb('api.R'); pr$run(host='0.0.0.0', port=as.numeric(Sys.getenv('PORT')))"]
