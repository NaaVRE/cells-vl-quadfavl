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
if (!requireNamespace("bioRad", quietly = TRUE)) {
	install.packages("bioRad", repos="http://cran.us.r-project.org")
}
library(bioRad)
if (!requireNamespace("glue", quietly = TRUE)) {
	install.packages("glue", repos="http://cran.us.r-project.org")
}
library(glue)
if (!requireNamespace("lubridate", quietly = TRUE)) {
	install.packages("lubridate", repos="http://cran.us.r-project.org")
}
library(lubridate)
if (!requireNamespace("purrr", quietly = TRUE)) {
	install.packages("purrr", repos="http://cran.us.r-project.org")
}
library(purrr)
if (!requireNamespace("stringr", quietly = TRUE)) {
	install.packages("stringr", repos="http://cran.us.r-project.org")
}
library(stringr)
if (!requireNamespace("jsonlite", quietly = TRUE)) {
	install.packages("jsonlite", repos="http://cran.us.r-project.org")
}
library(jsonlite)



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
conff_de_time_interval <- "720 mins"
conff_de_max_days <- 3

dir.create(file.path(conff_local_vp_dir), showWarnings = FALSE)



t<-seq(as.POSIXct(Sys.Date() - 1), as.POSIXct(Sys.Date()), conff_de_time_interval)
print(t)
conff_minio_endpoint <- "scruffy.lab.uvalight.net:9000"

expand_grid(odim=unlist(odimcode), times = t) |>
  mutate(
      times_utc=with_tz(times,'UTC'),
      filename=glue::glue("{odim}_vp_{strftime(times_utc, '%Y%m%dT%H%M%SZ_0xb.h5')}"),
      hdf5_dirpath=glue::glue("hdf5/{odim}/{strftime(times_utc, '%Y/%m/%d')}/"),
      local_path=file.path(conff_local_vp_dir,hdf5_dirpath,filename))|>
mutate(
      vp = purrr::pmap(
    list(odim, times, local_path),
    ~ list(try(calculate_vp(calculate_param(getRad::get_pvol(..1, ..2), RHOHV = urhohv), vpfile = ..3)))
  )
  )
print(res)
vp_paths <- (res$file)
# capturing outputs
print('Serialization of vp_paths')
file <- file(paste0('/tmp/vp_paths_', id, '.json'))
writeLines(toJSON(vp_paths, auto_unbox=TRUE), file)
close(file)
