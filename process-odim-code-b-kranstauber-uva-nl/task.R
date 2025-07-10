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
if (!requireNamespace("purrr", quietly = TRUE)) {
	install.packages("purrr", repos="http://cran.us.r-project.org")
}
library(purrr)
if (!requireNamespace("stringr", quietly = TRUE)) {
	install.packages("stringr", repos="http://cran.us.r-project.org")
}
library(stringr)
if (!requireNamespace("vol2birdR", quietly = TRUE)) {
	install.packages("vol2birdR", repos="http://cran.us.r-project.org")
}
library(vol2birdR)
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

{'name': 'conf_minio_endpoint', 'assignation': 'conf_minio_endpoint<-"scruffy.lab.uvalight.net:9000"'}
{'name': 'conf_local_vp_dir', 'assignation': 'conf_local_vp_dir<-"/tmp/data/vp"'}
{'name': 'conf_de_max_days', 'assignation': 'conf_de_max_days<-3'}
{'name': 'conf_de_time_interval', 'assignation': 'conf_de_time_interval<-"720 mins"'}

print("Running the cell")
print(odimcode)
print(dput(odimcode))
odimclean<-sub('\\]','',sub('\\[','',odimcode))
library("getRad")
library("tidyr")
library("dplyr")
library("bioRad")
stopifnot(length(odimclean)==1)
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

dir.create(file.path(conf_local_vp_dir), showWarnings = FALSE)

v2bversion <- format_v2b_version(vol2birdR::vol2bird_version())

wmocode <- getRad::get_weather_radars() |>
  filter(odimcode == odimclean, status==1) |>
  pull(wmocode)

t<-seq(as.POSIXct(Sys.Date() - 1), as.POSIXct(Sys.Date()), conf_de_time_interval)
print(t)
print("wmo")
print(wmocode)

res<-expand_grid(odim=unlist(odimclean), times = t) |>
  expand_grid(wmocode = wmocode) |>
  expand_grid(v2bversion = v2bversion) |>
  mutate(file = file.path(conf_local_vp_dir, generate_vp_file_name(odim, times, wmocode, v2bversion)),
         vp = purrr::pmap(
    list(odim, times, file),
    ~ calculate_vp(calculate_param(getRad::get_pvol(..1, ..2), RHOHV = urhohv), vpfile = ..3)
  ) ) 
print(res)
vp_paths <- (res$file)
# capturing outputs
print('Serialization of vp_paths')
file <- file(paste0('/tmp/vp_paths_', id, '.json'))
writeLines(toJSON(vp_paths, auto_unbox=TRUE), file)
close(file)
