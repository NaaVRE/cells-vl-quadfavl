setwd('/app')
library(optparse)
library(jsonlite)

if (!requireNamespace("SecretsProvider", quietly = TRUE)) {
	install.packages("SecretsProvider", repos="http://cran.us.r-project.org")
}
library(SecretsProvider)
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


secret_minio_access_key = Sys.getenv('secret_minio_access_key')
secret_minio_secret_key = Sys.getenv('secret_minio_secret_key')

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
print("dput")
print(secret_minio_access_key)
print(secret_minio_secret_key)

library("jsonlite")
dput(vp_paths)

a<-gsub('\\\\','"',vp_paths)
print("dput a")
dput(a)
aa<-c(jsonlite::fromJSON(a))
print("aa")
dput(aa)
print("for loop")
for (vp_path in aa){
    print(vp_path)
    print(file.exists(vp_path))
}
libary("bioRad")
vpts<-bind_into_vpts(read_vpfiles(aa[1:3]))
plot(vpts)
