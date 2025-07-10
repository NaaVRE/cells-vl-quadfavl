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

id <- gsub('"', '', opt$id)

{'name': 'conf_minio_endpoint', 'assignation': 'conf_minio_endpoint<-"scruffy.lab.uvalight.net:9000"'}
{'name': 'conf_local_vp_dir', 'assignation': 'conf_local_vp_dir<-"/tmp/data/vp"'}
{'name': 'conf_de_max_days', 'assignation': 'conf_de_max_days<-3'}
{'name': 'conf_de_time_interval', 'assignation': 'conf_de_time_interval<-"720 mins"'}

print("Running the cell")
library("getRad")
library("tidyr")
library("dplyr")


odimcodes <- getRad::weather_radars() |>
    dplyr::filter(
        country == param_country, status == 1
    ) |>
    dplyr::pull(`odimcode`)
odimcodes <- (odimcodes[1:3])
# capturing outputs
print('Serialization of odimcodes')
file <- file(paste0('/tmp/odimcodes_', id, '.json'))
writeLines(toJSON(odimcodes, auto_unbox=TRUE), file)
close(file)
