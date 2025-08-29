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
if (!requireNamespace("tibble", quietly = TRUE)) {
	install.packages("tibble", repos="http://cran.us.r-project.org")
}
library(tibble)
if (!requireNamespace("httr", quietly = TRUE)) {
	install.packages("httr", repos="http://cran.us.r-project.org")
}
library(httr)
if (!requireNamespace("xml2", quietly = TRUE)) {
	install.packages("xml2", repos="http://cran.us.r-project.org")
}
library(xml2)


secret_minio_key = Sys.getenv('secret_minio_key')
secret_minio_secret = Sys.getenv('secret_minio_secret')

print('option_list')
option_list = list(

make_option(c("--odimcode"), action="store", default=NA, type="character", help="my description"),
make_option(c("--param_default_hours_back"), action="store", default=NA, type="integer", help="my description"),
make_option(c("--param_n_vp"), action="store", default=NA, type="integer", help="my description"),
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
print("Retrieving param_default_hours_back")
var = opt$param_default_hours_back
print(var)
var_len = length(var)
print(paste("Variable param_default_hours_back has length", var_len))

param_default_hours_back = opt$param_default_hours_back
print("Retrieving param_n_vp")
var = opt$param_n_vp
print(var)
var_len = length(var)
print(paste("Variable param_n_vp has length", var_len))

param_n_vp = opt$param_n_vp
id <- gsub('"', '', opt$id)

conf_time_interval<-"5 mins"
conf_minio_region<-"nl-uvalight"
conf_minio_bucket<-"naa-vre-public"
conf_minio_endpoint<-"scruffy.lab.uvalight.net:9000"
conf_minio_main_path<-"vl-vol2bird/quadfavl/"
conf_local_vp_dir<-"/tmp/data/vp"

print("Running the cell")
cli::cli_h3("{.arg odimcode} before cleaning")
dput(odimcode)

odimcode <- gsub("\\[|\\]", "", odimcode)
cli::cli_h3("{.arg odimcode} after cleaning")
dput(odimcode)

library("getRad")
library("tidyr")
library("dplyr")
library("bioRad")
library("glue")
library("lubridate")
library("magrittr")
sessionInfo()
stopifnot(length(odimcode) == 1)
invisible(conf_minio_main_path) # seems code analyzyser missed this config
invisible(conf_minio_region)
invisible(conf_minio_endpoint)
invisible(conf_minio_bucket)

dir.create(file.path(conf_local_vp_dir), showWarnings = FALSE)

cli::cli_h1("Creating time sequence")
time <- lubridate::with_tz(lubridate::floor_date(Sys.time(), conf_time_interval), "UTC")
hours_back<-  switch(substr(odimcode, 1, 2),
    "se" = 23,
    param_default_hours_back
  )
t <- seq(time - lubridate::hours(hours_back), time, conf_time_interval)
cli::cli_inform("Times: {t}")
cli::cli_h1("Creating {.cls data.frame} with jobs")

planned_work <- 
  expand_grid(odim = unlist(odimcode), times = t) |>
  mutate(
    times_utc = with_tz(times, "UTC"),
    filename = glue::glue("{odim}_vp_{strftime(times_utc, '%Y%m%dT%H%M%SZ_0xb.h5')}"),
    hdf5_dirpath = glue::glue("hdf5/{odim}/{strftime(times_utc, '%Y/%m/%d')}/"),
    local_path = gsub("//", "/", file.path(conf_local_vp_dir, hdf5_dirpath, filename))
  ) |>
  group_by(hdf5_dirpath) |>
  group_walk(~ {
    dir.create(file.path(conf_local_vp_dir, .y$hdf5_dirpath), recursive = T, showWarnings = FALSE)
  }) |>
  group_modify(~ {
    aws.s3::get_bucket(
      bucket = conf_minio_bucket,
      prefix = paste0(conf_minio_main_path, .y),
      delimiter = "/",
      use_https = TRUE,
      check_region = FALSE,
      region = conf_minio_region,
      verbose = FALSE,
      parse_response = TRUE,
      base_url = conf_minio_endpoint,
      key = secret_minio_key,
      secret = secret_minio_secret
    ) |>
      purrr::map_chr(~ .x$Key) |>
      basename() -> existing_files
    .x %>% tibble::add_column(file_exists = .x$filename %in% existing_files)
  }) |>
  ungroup() %T>%
  {
    x <- .
    cli::cli_inform("Out of {nrow(x)} files {sum(x$file_exists)} already exist")
  } |>
  filter(!file_exists)

convert_pvol_for_vp_calculations <- function(pvol) {
  stopifnot(bioRad::is.pvol(pvol))
  ctry_code <- substr(pvol$radar, 1, 2)
  switch(ctry_code,
    "de" = list(default=calculate_param(pvol, RHOHV = urhohv)),
    "cz" = list(default=pvol, th=calculate_param(pvol,DBZH=TH)),
    "ro" = list(default=pvol, singlepol=dplyr::select(pvol, -RHOHV)),
    "se" = list(default=pvol, 
                ccorh=calculate_param(pvol, DBZH = TH-CCORH), 
                ccorh_cpa=calculate_param(pvol,DBZH=dplyr::if_else(c(CPA)>.75, NA, c(TH-CCORH)))),
    list(default=pvol)
  )
}
res <- data.frame()
while (sum(purrr::map_lgl(res$vp, is.character)) < param_n_vp & nrow(planned_work) != 0) {
  res <- planned_work |>
    head(param_n_vp - sum(purrr::map_lgl(res$vp, bioRad::is.vp))) |>
    mutate(
      vp = purrr::pmap(
        list(odim, times, local_path),~try({
            pvolList<-convert_pvol_for_vp_calculations(getRad::get_pvol(..1, ..2, param="all"))
            vpPaths<-c()
            for( i in names(pvolList)){
                if(i =="default"){
                    vpFilePath<- ..3
                }else{
                    vpFilePath<-sub(paste0(conf_local_vp_dir,'/hdf5/'),paste0(conf_local_vp_dir,'/hdf5/',i,'/'),..3)
                    dir.create(dirname(vpFilePath), recursive = T, showWarnings = FALSE)
                }
                suppressMessages(calculate_vp(pvolList[[i]], vpfile = vpFilePath))
                vpPaths<-c(vpPaths, vpFilePath)
            }
            vpPaths
        }),
        .progress = list(
          type = "iterator",
          format = "Calculating vertical profiles {cli::pb_bar} {cli::pb_percent}",
          clear = TRUE
        )
      )
    ) |>
    bind_rows(res)
  planned_work <- planned_work |> filter(!(filename %in% res$filename))
}


failed <- purrr::map_lgl(res$vp, inherits, "try-error")
if (any(failed)) {
  cli::cli_alert_danger("There are failed jobs ({sum(failed)}/{nrow(res)})")
  cli::cli_alert_info("The following files are omited {res$filename[failed]}")
  res <- res[!failed, ]
}
print(res)
vp_paths <- gsub(paste0(conf_local_vp_dir,"/hdf5/"), "", unlist(res$vp))
# capturing outputs
print('Serialization of vp_paths')
file <- file(paste0('/tmp/vp_paths_', id, '.json'))
writeLines(toJSON(vp_paths, auto_unbox=TRUE), file)
close(file)
