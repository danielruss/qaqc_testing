FROM rocker/tidyverse:latest
<<<<<<< HEAD
<<<<<<< HEAD
RUN install2.r --error plumber rlang readxl rlang
=======
RUN install2.r rio plumber bigrquery rlang
>>>>>>> d852c60341857a5075ff69908818c5c988786d9a
COPY ["./qaqc.R", "./qaqc.R"]
COPY ["./qc_rules.xlsx", "./qc_rules.xlsx"]
ENTRYPOINT ["R", "-e", "pr <- plumber::plumb('qaqc.r'); pr$run(host='0.0.0.0', port=as.numeric(Sys.getenv('PORT')))"]
=======
RUN install2.r plumber readxl tidyverse rlang bigrquery
COPY ["./qaqc.R", "./qaqc.R"]
COPY ["./qc_rules.xlsx", "./qc_rules.xlsx"]
ENTRYPOINT ["R", "-e","pr <- plumber::plumb('qaqc.R'); pr$run(host='0.0.0.0', port=as.numeric(Sys.getenv('PORT')))"]
>>>>>>> 7f4fcd4fbe0d5ebbe2acea37667c75d5079049a6
