suppressPackageStartupMessages(library(arrow))



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

diretorioDest <- paste0(diretorioBase,diretorioPQs)

if (!file_test("-d",diretorioDest)) {
  system(paste0("mkdir -p ",diretorioDest))
}

print("===================START==========================")
print(Sys.time())
print(paste0("Starting conversion for UFs: ", paste(estados, collapse=" ")))
print(paste0("Anos: ", paste(anos, collapse=" ")))
print(paste0("Working with ", PREF, " files."))

for(UF in estados) {
  for(ANO in anos) {
    tryCatch(expr = 
               {
                 # verifica se a data do arquivo Parquet é mais velha do que
                 # o arquivo DBC mais recente daquele ano.
                 arquivosUFANO <- list.files(diretorioBase,paste0(PREF,UF,ANO,MESES,".*.dbc"))
                 finfUFANO <- file.info(arquivosUFANO, extra_cols = FALSE)
                 mtimes <- finfUFANO[,"mtime"]
                 ultimdata <- max(mtimes)
                 caminhohive <- paste0("ESTADO=",UF,"/ANO=",paste0("20",ANO))
                 arquivosPQ <- list.files(file.path(diretorioDest,caminhohive), "/*.parquet",full.names = TRUE)
                 if (length(arquivosPQ) > 0) {
                  fpqsInfo <- file.info(arquivosPQ, extra_cols = FALSE)
                  pqtimes <- fpqsInfo[,"mtime"]
                  dtpqs <- max(pqtimes)
                  difdata <- difftime(ultimdata, dtpqs, units = "days") > 0 # se o DBC for mais recente, recria o Parquet
                 } else {
                   difdata <- TRUE # não existe o parquet, cria
                 }
                 if(difdata) {
                    # se o DBC for mais recente, recria o Parquet
                    # vou remover os arquivos Parquet existentes antes de criar novos
                    if (!testcfg) {
                      if (delOldPQfiles) {
                        logfunc(paste0("Removing old Parquet files: ", paste(arquivosPQ, collapse = " ")))
                        file.remove(arquivosPQ)
                      } else { # renaming old Parquet files
                        oldPQfiles <- stringr::str_replace(arquivosPQ, ".parquet","-old.parquet")
                        logfunc(paste0("Renaming old Parquet files: ", paste(oldPQfiles, collapse = " ")))
                        for (j in 1:lenght(oldPQfiles)) {
                          file.rename(arquivosPQ[j],oldPQfiles[j])
                        }
                      }
                    } else {
                      if (delOldPQfiles) {
                        logfunc(paste0("Would remove Parquet files: ", paste(arquivosPQ, collapse = " ")))
                      } else { # renaming old Parquet files
                        oldPQfiles <- stringr::str_replace(arquivosPQ, ".parquet","-old.parquet")
                        logfunc(paste0("Would rename old Parquet files: ", paste(oldPQfiles, collapse = " ")))
                      }
                    }
                    # se o Parquet não existir, cria.
                    resultado <- converteDBCtoPQ(PREF,ANO,diretorioBase,UF,MESES,diretorioPQs)
                    
                    logfunc(paste0("Conversion for ", UF, "; Ano: 20", ANO, "; Meses: ", MESES,". Done!"))
                 } else {
                   logfunc(paste0("Conversion for ", UF, ", Ano: 20",ANO, ", Meses:", MESES, ", not necessary."))
                 }
               }, 
             error = function(e) {
               print(paste0(UF, " ", ANO, " error message: ", e))
             }
    )       
  }
}

print(Sys.time())
print("==================END========================")

