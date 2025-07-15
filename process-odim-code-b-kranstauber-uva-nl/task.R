setwd('/app')
library(optparse)
library(jsonlite)

if (!requireNamespace("dplyr", quietly = TRUE)) {
	install.packages("dplyr", repos="http://cran.us.r-project.org")
}
library(dplyr)
if (!requireNamespace("getRad", quietly = TRUE)) {
	install.packages("getRad", repos="http://cran.us.r-project.org")
}
library(getRad)
if (!requireNamespace("tidyr", quietly = TRUE)) {
	install.packages("tidyr", repos="http://cran.us.r-project.org")
}
library(tidyr)
if (!requireNamespace("aws.s3", quietly = TRUE)) {
	install.packages("aws.s3", repos="http://cran.us.r-project.org")
}
library(aws.s3)
if (!requireNamespace("bioRad", quietly = TRUE)) {
	install.packages("bioRad", repos="http://cran.us.r-project.org")
}
library(bioRad)
if (!requireNamespace("cli", quietly = TRUE)) {
	install.packages("cli", repos="http://cran.us.r-project.org")
}
library(cli)
if (!requireNamespace("glue", quietly = TRUE)) {
	install.packages("glue", repos="http://cran.us.r-project.org")
}
library(glue)
if (!requireNamespace("lubridate", quietly = TRUE)) {
	install.packages("lubridate", repos="http://cran.us.r-project.org")
}
library(lubridate)
if (!requireNamespace("magrittr", quietly = TRUE)) {
	install.packages("magrittr", repos="http://cran.us.r-project.org")
}
library(magrittr)
if (!requireNamespace("purrr", quietly = TRUE)) {
	install.packages("purrr", repos="http://cran.us.r-project.org")
}
library(purrr)
if (!requireNamespace("stringr", quietly = TRUE)) {
	install.packages("stringr", repos="http://cran.us.r-project.org")
}
library(stringr)
if (!requireNamespace("tibble", quietly = TRUE)) {
	install.packages("tibble", repos="http://cran.us.r-project.org")
}
library(tibble)
if (!requireNamespace("jsonlite", quietly = TRUE)) {
	install.packages("jsonlite", repos="http://cran.us.r-project.org")
}
library(jsonlite)


secret_minio_key = Sys.getenv('secret_minio_key')
secret_minio_secret = Sys.getenv('secret_minio_secret')

print('option_list')
option_list = list(

make_option(c("--odimcode"), action="store", default=NA, type="character", help="my description"),
make_option(c("--id"), action="store", default=NA, type="character", help="task id")
)


opt = parse_args(OptionParser(option_list=option_list))

var_serialization <- function(var){
    if (is.null(var)){
        print("Variable is null")
        exit(1)
    }
    tryCatch(
        {
            var <- fromJSON(var)
            print("Variable deserialized")
            return(var)
        },
        error=function(e) {
            print("Error while deserializing the variable")
            print(var)
            var <- gsub("'", '"', var)
            var <- fromJSON(var)
            print("Variable deserialized")
            return(var)
        },
        warning=function(w) {
            print("Warning while deserializing the variable")
            var <- gsub("'", '"', var)
            var <- fromJSON(var)
            print("Variable deserialized")
            return(var)
        }
    )
}

print("Retrieving odimcode")
var = opt$odimcode
print(var)
var_len = length(var)
print(paste("Variable odimcode has length", var_len))

odimcode <- gsub("\"", "", opt$odimcode)
id <- gsub('"', '', opt$id)


print("Running the cell")
cli::cli_h3("{.arg odimcode} before cleaning")
dput(odimcode)

odimcode<-gsub('\\[|\\]','', odimcode)
cli::cli_h3("{.arg odimcode} after cleaning")
dput(odimcode)

library("getRad")
library("tidyr")
library("dplyr")
library("bioRad")
library("glue")
library("lubridate")
stopifnot(length(odimcode)==1)
format_v2b_version <- function(vol2bird_version) {
  v2b_version_formatted <- gsub(".", "-", vol2bird_version, fix = TRUE)
  v2b_version_parts <- stringr:::str_split(v2b_version_formatted, pattern = "-")
  v2b_major_version_parts <- unlist(v2b_version_parts)[1:3]
  v2b_major_version_formatted <- paste(
    c(
      "v",
      paste(
        v2b_major_version_parts,
        collapse = "-"
      ),
      ".h5"
    ),
    collapse = ""
  )
  return(v2b_major_version_formatted)
}

generate_vp_file_name <- function(odimcode, times, wmocode, v2bversion) {
  datatype <- "vp"
  formatted_time <- format(times, format = "%Y%m%dT%H%M", tz = "UTC", usetz = FALSE)
  filename <- paste(
    odimcode, datatype, formatted_time, wmocode, v2bversion,
    sep = "_"
  )
  print(filename)
  return(filename)
}
conff_local_vp_dir <- "/tmp/data/vp"
conff_de_time_interval <- "5 mins"
conff_de_max_days <- 3

dir.create(file.path(conff_local_vp_dir), showWarnings = FALSE)



cli::cli_h1("Creating time sequence")

t<-seq(as.POSIXct(Sys.Date() - 2), as.POSIXct(Sys.Date()-1), conff_de_time_interval)
print(t)
conff_minio_endpoint <- "scruffy.lab.uvalight.net:9000"
cli::cli_h1("Creating {.cls data.frame} with jobs")
require(magrittr)
res<-expand_grid(odim=unlist(odimcode), times = t) |>
  mutate(
      times_utc=with_tz(times,'UTC'),
      filename=glue::glue("{odim}_vp_{strftime(times_utc, '%Y%m%dT%H%M%SZ_0xb.h5')}"),
      hdf5_dirpath=glue::glue("hdf5/{odim}/{strftime(times_utc, '%Y/%m/%d')}/"),
      local_path= gsub('//','/',file.path(conff_local_vp_dir,hdf5_dirpath,filename)))|>
group_by(hdf5_dirpath)|>
group_walk(~{dir.create(file.path(conff_local_vp_dir, .y$hdf5_dirpath), recursive = T, showWarnings=FALSE)}) |>
  group_modify(~ {
      aws.s3::get_bucket(
  bucket = "naa-vre-public",
  prefix = paste0("vl-vol2bird/quadfavl/", .y),
  delimiter = "/",
  use_https = T,
  check_region = F,
  region = "nl-uvalight",
  verbose = FALSE,
  parse_response = T,
  base_url = "scruffy.lab.uvalight.net:9000",
  key = secret_minio_key,
  secret = secret_minio_secret
  ) |> purrr::map_chr(~.x$Key) |> basename() -> existing_files
    .x %>% tibble::add_column(file_exists=.x$filename %in% existing_files)
  }) |>
ungroup()%T>% {x<-.;cli::cli_inform("Out of {nrow(x)} files {sum(x$file_exists)} already exist")} |> 
filter(!file_exists)|>

mutate(
      vp = purrr::pmap(
    list(odim, times, local_path),
    ~ suppressMessages(try(calculate_vp(calculate_param(getRad::get_pvol(..1, ..2), RHOHV = urhohv), vpfile = ..3))),
          .progress = list(
  type = "iterator", 
  format = "Calculating vertical profiles {cli::pb_bar} {cli::pb_percent}",
  clear = TRUE)
  )
  )

failed<-purrr::map_lgl(res$vp, inherits, "try-error")
if(any(failed))
    {
    cli::cli_alert_danger("There are failed jobs ({sum(failed)}/{nrow(res)})")
    cli::cli_alert_info("The following files are omited {res$filename[failed]}")
    res<-res[!failed,]
    }
print(res)
vp_paths <- res$local_path
# capturing outputs
print('Serialization of vp_paths')
file <- file(paste0('/tmp/vp_paths_', id, '.json'))
writeLines(toJSON(vp_paths, auto_unbox=TRUE), file)
close(file)
