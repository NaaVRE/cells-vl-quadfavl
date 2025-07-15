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

make_option(c("--vp_paths"), action="store", default=NA, type="character", help="my description"),
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

print("Retrieving vp_paths")
var = opt$vp_paths
print(var)
var_len = length(var)
print(paste("Variable vp_paths has length", var_len))

vp_paths <- gsub("\"", "", opt$vp_paths)
id <- gsub('"', '', opt$id)


print("Running the cell")
library("jsonlite")

cli::cli_h3("{.arg vp_paths} before cleaning")
dput(vp_paths)
vp_paths<-gsub(' |\\[|\\]','',strsplit(vp_paths,',')[[1]])

cli::cli_h3("{.arg vp_paths} after cleaning")
dput(vp_paths)
if(length(vp_paths)!=1 || vp_paths!=""){
vp_paths<-paste0("/tmp/data/vp/hdf5/", vp_paths)
Sys.setenv(
  AWS_ACCESS_KEY_ID = secret_minio_key,
  AWS_SECRET_ACCESS_KEY = secret_minio_secret,
  AWS_S3_ENDPOINT = "scruffy.lab.uvalight.net:9000",
  AWS_DEFAULT_REGION = "nl-uvalight"
)

conff_local_vp_dir <- "/tmp/data/vp"

cli::cli_progress_bar( format = paste0(
  "{pb_spin} Uploading {.path {basename(vp_path)}}",
  "[{pb_current}/{pb_total}]   ETA:{pb_eta}"
), total = length(vp_paths))
for (vp_path in vp_paths){
        object<-sub(paste0(conff_local_vp_dir,'/'),'vl-vol2bird/quadfavl/',vp_path)  
      cli::cli_progress_update()
    aws.s3::put_object(file=vp_path,
  bucket = "naa-vre-public",
                 object=object,
  delimiter = "/",
  use_https = T,
  check_region = F,
  verbose = FALSE,

  
) 
   
}
cli::cli_process_done()
}
dummy<-"TRUE"
# capturing outputs
print('Serialization of dummy')
file <- file(paste0('/tmp/dummy_', id, '.json'))
writeLines(toJSON(dummy, auto_unbox=TRUE), file)
close(file)
