FROM rocker/tidyverse:4.3
RUN install2.r plyr rio plumber bigrquery googleCloudStorageR gargle janitor rlang config writexl
COPY ["./qaqc.R", "./qaqc.R"]
COPY ["./api.R",  "./api.R"]
COPY ["./prod/config.yml", "./config.yml"] # copy config.yml from local prod directory to main directory in cloud run
COPY ["./get_merged_module_1_data.R", "./get_merged_module_1_data.R"]
COPY ["./get_merged_module_2_data.R", "./get_merged_module_2_data.R"]
COPY ["./get_merged_module_3_data.R", "./get_merged_module_3_data.R"]
COPY ["./get_merged_module_4_data.R", "./get_merged_module_4_data.R"]
COPY ["./get_merged_rca_data.R", "./get_merged_rca_data.R"]
COPY ["./get_merged_biospecimen_and_recruitment_data.R", "./get_merged_biospecimen_and_recruitment_data.R"]
COPY ["./check_and_correct_exceptions.R", "./check_and_correct_exceptions.R"]
COPY ["./qc_rules_recruitment.xlsx", "./qc_rules_recruitment.xlsx"]
COPY ["./qc_rules_biospecimen.xlsx", "./qc_rules_biospecimen.xlsx"]
COPY ["./qc_rules_module1.xlsx", "./qc_rules_module1.xlsx"]
COPY ["./qc_rules_module2.xlsx", "./qc_rules_module2.xlsx"]
COPY ["./qc_rules_module3.xlsx", "./qc_rules_module3.xlsx"]
COPY ["./qc_rules_module4.xlsx", "./qc_rules_module4.xlsx"]
COPY ["./qc_rules_rca.xlsx", "./qc_rules_rca.xlsx"]
COPY ["./exclusions/qc_exclusions_recruitment.csv", "./exclusions/qc_exclusions_recruitment.csv"]

ENTRYPOINT ["R", "-e", "pr <- plumber::plumb('api.R'); pr$run(host='0.0.0.0', port=as.numeric(Sys.getenv('PORT')))"]
