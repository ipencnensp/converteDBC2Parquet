# This is a spare script that may help those who want to convert DBC files
# to CSV instead to Parquet.
# One CSV files will be created for each year
# You have to define `diretorioDest` to where you want to save the CSVs files


library(data.table)


getScriptPath <- function(){
  cmd.args <- commandArgs()
  m <- regexpr("(?<=^--file=).+", cmd.args, perl=TRUE)
  script.dir <- dirname(regmatches(cmd.args, m))
  if(length(script.dir) == 0) stop("can't determine script dir: please call the script with Rscript")
  if(length(script.dir) > 1) stop("can't determine script dir: more than one '--file' argument detected")
  return(script.dir)
}

logfunc <- function(mensagem="Working") {
  if (loglevel != 0) {
    print(mensagem)
  } 
}

scriptspath <- getScriptPath()

source(file.path(scriptspath,"config.R"))

source(file.path(scriptspath,"converteDBC.R"))

diretorioDest <- "CSVs"
diretorioDest <- paste0(diretorioBase,diretorioDest)

if (!file_test("-d",diretorioDest)) {
  system(paste0("mkdir -p ",diretorioDest))
}

for(UF in estados) {
  for(ANO in anos) {
    tryCatch(expr = 
               {
                 df <- converteDBCtoDF(PREF,ANO,diretorioBase,UF,MESES) #%>%
                 nomearquivocsv = paste0(PREF,UF,"_20",ANO,".csv")
                 fwrite(df, file.path(diretorioDest,nomearquivocsv))
                 rm(df)         
               }, 
             error = function(e) {
               print(paste0(UF, " ", ANO, " error message: ", e))
             }
    )       
  }
}


