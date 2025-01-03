get_merged_rca_data <- function(project){
  sql <- glue::glue(
   "SELECT
      t.*,
      p.d_827220437 -- get site_id
    FROM `{project}.FlatConnect.cancerOccurrence_JP` AS t
    JOIN `{project}.FlatConnect.participants_JP` AS p
      ON t.Connect_ID = p.Connect_ID")

  data_query <- bq_project_query(project, query = sql)
  data <- bq_table_download(data_query, bigint = "integer64")

  print(glue("length data: {length(data)}"))
  return(data)
}
