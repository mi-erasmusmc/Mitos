#' Export phenotypes from the OHDSI PhenotypeLibrary into local JSON files.
#'
#' Usage:
#'   Rscript scripts/export_phenotypes.R --ids 1,2,3 --out fixtures/phenotypes

suppressPackageStartupMessages({
  library(optparse)
  library(jsonlite)
  library(PhenotypeLibrary)
})

option_list <- list(
  optparse::make_option(
    "--ids",
    type = "character",
    help = "Comma-separated list of phenotype IDs to download (required)."
  ),
  optparse::make_option(
    "--out",
    type = "character",
    default = "fixtures/phenotypes",
    help = "Output directory for cohort JSON files."
  )
)

opts <- optparse::parse_args(optparse::OptionParser(option_list = option_list))

if (is.null(opts$ids)) {
  stop("--ids must be provided (comma separated list of phenotype IDs).")
}

dir.create(opts$out, recursive = TRUE, showWarnings = FALSE)

parse_id_token <- function(token) {
  token <- trimws(token)
  if (grepl(":", token, fixed = TRUE)) {
    parts <- strsplit(token, ":", fixed = TRUE)[[1]]
    if (length(parts) != 2) {
      stop(sprintf("Invalid range token: %s", token))
    }
    start <- as.numeric(parts[1])
    end <- as.numeric(parts[2])
    if (is.na(start) || is.na(end)) {
      stop(sprintf("Invalid numeric range token: %s", token))
    }
    return(seq(start, end))
  }
  value <- as.numeric(token)
  if (is.na(value)) {
    stop(sprintf("Invalid phenotype id: %s", token))
  }
  value
}

raw_tokens <- trimws(strsplit(opts$ids, ",")[[1]])
ids_numeric <- unlist(lapply(raw_tokens, parse_id_token))

definitions <- PhenotypeLibrary::getPlCohortDefinitionSet(ids_numeric)

for (row in seq_len(nrow(definitions))) {
  phenotype_id <- definitions$cohortId[row]
  message("Writing phenotype ", phenotype_id)
  json_str <- definitions$json[row]
  pretty_json <- jsonlite::prettify(json_str, indent = 2)
  out_path <- file.path(opts$out, sprintf("phenotype-%s.json", phenotype_id))
  writeLines(pretty_json, out_path)
}

message("Export complete.")
