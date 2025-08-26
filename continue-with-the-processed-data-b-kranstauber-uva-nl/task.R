setwd('/app')
library(optparse)
library(jsonlite)




print('option_list')
option_list = list(

make_option(c("--processed_c_data"), action="store", default=NA, type="character", help="my description"),
make_option(c("--processed_data"), action="store", default=NA, type="character", help="my description"),
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

print("Retrieving processed_c_data")
var = opt$processed_c_data
print(var)
var_len = length(var)
print(paste("Variable processed_c_data has length", var_len))

processed_c_data <- gsub("\"", "", opt$processed_c_data)
print("Retrieving processed_data")
var = opt$processed_data
print(var)
var_len = length(var)
print(paste("Variable processed_data has length", var_len))

print("------------------------Running var_serialization for processed_data-----------------------")
print(opt$processed_data)
processed_data = var_serialization(opt$processed_data)
print("---------------------------------------------------------------------------------")

id <- gsub('"', '', opt$id)


print("Running the cell")
print("This is list() processed_data:")
print(processed_data)

print("This is c() processed_data:")
print(processed_c_data)
