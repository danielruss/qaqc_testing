get_merged_biospecimen_and_recruitment_data <- function(project) {
  billing <- project
  data_query <- bq_project_query(
    project,
    query = glue(
"WITH
  PART AS (
  SELECT
    *
  FROM
    `nih-nci-dceg-connect-prod-6d04.FlatConnect.participants_JP`
  WHERE
    CONNECT_ID IS NOT NULL),

  BIO AS (
  SELECT
    *
  FROM
    `nih-nci-dceg-connect-prod-6d04.FlatConnect.biospecimen_JP`)

SELECT
  *
FROM
  BIO
LEFT JOIN PART
ON PART.Connect_ID = BIO.Connect_ID"
    )
  )
  data <- bq_table_download(data_query, bigint = "integer64")
}