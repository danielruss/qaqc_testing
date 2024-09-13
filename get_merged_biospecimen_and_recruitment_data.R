get_merged_biospecimen_and_recruitment_data <- 
  function(project, exclude_duplicates=FALSE) {
  billing <- project
  
  sql1 <- "WITH
            PART AS (SELECT * FROM `{project}.FlatConnect.participants_JP`
                     WHERE Connect_ID IS NOT NULL AND d_831041022='104430631'),
            BIO AS (SELECT * FROM `{project}.FlatConnect.biospecimen_JP` WHERE Connect_ID IS NOT NULL)
           SELECT * FROM BIO LEFT JOIN PART ON PART.Connect_ID = BIO.Connect_ID"
  
  data_query <- bq_project_query(project, query = glue(sql1))
  data <- bq_table_download(data_query, bigint = "integer64", page_size = 1000)
  
  print(glue("length data: {length(data)}"))
  
  if (exclude_duplicates==TRUE) {
    
    # This query gets a list of Connect_IDs with multiple entries
    sql2 <- "SELECT Connect_ID FROM `{project}.FlatConnect.biospecimen_JP` WHERE Connect_ID IS NOT NULL
             GROUP BY Connect_ID HAVING COUNT(Connect_ID) > 1"
    
    # This gets a list of Connect_IDs with multiple entries
    exclusion_query <- bq_project_query(project, query = glue(sql2))
    exclusion_data   <- bq_table_download(exclusion_query, bigint = "integer64")
    Connect_IDs_to_exclude <- exclusion_data$Connect_ID
    print(Connect_IDs_to_exclude)
  
    data <- data %>% filter(!(Connect_ID %in% Connect_IDs_to_exclude))
  }
  print(glue("length data after filter: {length(data)}"))
  return(data)
}
