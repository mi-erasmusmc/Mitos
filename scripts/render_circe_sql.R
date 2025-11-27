suppressPackageStartupMessages({
  library(CirceR)
  library(SqlRender)
})

args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 3) {
  stop("Usage: Rscript scripts/render_circe_sql.R <json> <target_schema> <target_table> [output]")
}

json_path <- args[[1]]
target_schema <- args[[2]]
target_table <- args[[3]]
output_path <- if (length(args) >= 4) args[[4]] else NULL

json_str <- paste(readLines(json_path, warn = FALSE), collapse = "\n")
expression <- CirceR::cohortExpressionFromJson(json_str)
options <- CirceR::createGenerateOptions()
options$generateStats <- FALSE
options$useTempTables <- FALSE
options$tempEmulationSchema <- target_schema

sql <- CirceR::buildCohortQuery(expression, options)
sql <- SqlRender::render(
  sql,
  cdm_database_schema = target_schema,
  vocabulary_database_schema = target_schema,
  results_database_schema = target_schema,
  target_database_schema = target_schema,
  target_cohort_table = target_table,
  target_cohort_id = 1,
  tempEmulationSchema = target_schema
)
sql <- SqlRender::translate(sql = sql, targetDialect = "duckdb")

if (!is.null(output_path)) {
  writeLines(sql, output_path)
} else {
  cat(sql)
}
