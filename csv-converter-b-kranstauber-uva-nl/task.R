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
if (!requireNamespace("httr", quietly = TRUE)) {
	install.packages("httr", repos="http://cran.us.r-project.org")
}
library(httr)
if (!requireNamespace("xml2", quietly = TRUE)) {
	install.packages("xml2", repos="http://cran.us.r-project.org")
}
library(xml2)
if (!requireNamespace("SecretsProvider", quietly = TRUE)) {
	install.packages("SecretsProvider", repos="http://cran.us.r-project.org")
}
library(SecretsProvider)


secret_minio_key = Sys.getenv('secret_minio_key')
secret_minio_secret = Sys.getenv('secret_minio_secret')

print('option_list')
option_list = list(

make_option(c("--dummy"), action="store", default=NA, type="character", help="my description"),
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

print("Retrieving dummy")
var = opt$dummy
print(var)
var_len = length(var)
print(paste("Variable dummy has length", var_len))

dummy <- gsub("\"", "", opt$dummy)
id <- gsub('"', '', opt$id)


print("Running the cell")
Sys.setenv(
  AWS_ACCESS_KEY_ID = secret_minio_key,
  AWS_SECRET_ACCESS_KEY = secret_minio_secret,
  AWS_S3_ENDPOINT = "scruffy.lab.uvalight.net:9000",
  AWS_DEFAULT_REGION = "nl-uvalight"
)




aws.s3::get_bucket(
  bucket = "naa-vre-public",
  prefix = paste0("vl-vol2bird/quadfavl/hdf5/"),
  delimiter = "/",
  use_https = T,
  check_region = F,
  verbose = F,
  parse_response = F
  ) |> httr::content(as="parsed") |>xml2::xml_ns_strip() |>xml2::xml_find_all("CommonPrefixes/Prefix|Contents/Key") |> xml2::xml_text() |> basename()-> odim
odim
require(dplyr)
require(tidyr)
df<-expand_grid(odim=odim, date=Sys.Date()-0:4) |> 
mutate(
    hdfpath=glue::glue("hdf5/{odim}/{strftime(date, '%Y/%m/%d')}/"),
csvpath=glue::glue("daily/{odim}/{lubridate::year(date)}/{odim}_vpts_{strftime(date, '%Y%m%d')}.csv"),
hdfdf=purrr::map(hdfpath,~{
(    aws.s3::get_bucket_df(
  bucket = "naa-vre-public",
  prefix = paste0("vl-vol2bird/quadfavl/",.x),
  delimiter = "/",
  use_https = T,
  check_region = F,
  verbose = F#,
  ))
}),
nhdffiles=purrr::map_int(hdfdf, nrow)) |>filter(nhdffiles>0) |> 
mutate(hdftime=lubridate::as_datetime(purrr::map_vec(hdfdf,~max(.x$LastModified))),
      hdfkeys=purrr::map(hdfdf,~(.x$Key)))|> rowwise() |> 
mutate(csvtime=if(!aws.s3::object_exists(  bucket = "naa-vre-public",object=paste0("vl-vol2bird/quadfavl/",csvpath))){Sys.time()-lubridate::days(10000)}else{
               lubridate::as_datetime(aws.s3::get_bucket_df(
  bucket = "naa-vre-public",
  prefix = paste0("vl-vol2bird/quadfavl/",csvpath),
  delimiter = "/",
  use_https = T,
  check_region = F,
  verbose = F#,
  )$LastModified)}
               ) |> filter(hdftime>csvtime)|>
select(-hdfdf) |> 
mutate(vpts=list(bind_rows(purrr::map(hdfkeys, ~as.data.frame(suntime=F,aws.s3::s3read_using(bioRad::read_vpfiles, object = .x, bucket = "naa-vre-public"))|> tibble::add_column(source_file=basename(.x))))))|>
mutate(aws.s3::s3write_using(vpts,write.csv, na='',row.names=F, quote=F,bucket = "naa-vre-public",
  object = paste0("vl-vol2bird/quadfavl/",csvpath)))

df|>select(-vpts, -hdfkeys)
