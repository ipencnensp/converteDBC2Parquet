suppressPackageStartupMessages(library(dplyr))
suppressPackageStartupMessages(library(foreign))
suppressPackageStartupMessages(library(read.dbc))
suppressPackageStartupMessages(library(data.table))
suppressPackageStartupMessages(library(parallel))
suppressPackageStartupMessages(library(doParallel))



# Usar apenas 3/4 dos cores disponíveis
if (!exists("numCores")) {
  numCores <- NULL
}
if (is.null(numCores)) {
  numCores <- as.integer(round(detectCores()*0.5, 0))
  #numCores <- 14
} else if (is.numeric(numCores)) {
  cluster <- makeCluster(numCores)
} else {
  numCores <- 1
  print("using only 1 core; \n if you want to use more, set numCores before sourcing this file \n or left it as NULL to use half of the available cores")
}




doParallel::registerDoParallel(cores = numCores)


converteDBCtoDF <- function(Prefixo, Ano, Diretorio, UFpar, MESESpar) {
  # Prefixo:  o tipo de APAC a converter (PA, AR, AQ, AM, AN, AB, etc)
  # Ano: em formato de string, dois digitos ("08", "12", etc)
  # Diretorio: caminho até os arquivos *.dbc
  # UFpar: qual estado a converter
  # MESESpar: quais meses a converter; default são todos os meses.
  # esta funcao faz a leitura dos arquivos *.dbc de uma determinada UF para os
  # MESES determinados por uma máscara do tipo (XX|XX|XX|XX|XX|XX) onde XX é 
  # uma string representando cada mês (dois dígitos). A função retorna um 
  # data.frame que pode então ser salvo em CSV no diretório apropriado
  
  arquivos <- list.files(Diretorio, paste0(Prefixo,UFpar,Ano,MESESpar,".*.dbc"))
  fNames <- lapply(arquivos, function(x) {file.path(Diretorio,x)})
  
  # tamanho do Chunk igual ao número de cores
  sChunk <- numCores
  N <- length(fNames)
  
  if(length(fNames) > sChunk) {
    nChunks <- floor(length(fNames)/sChunk)
  } else {
    nChunks <- 1
    sChunk <- length(fNames)
  }
  rChunks <- length(fNames) %% sChunk
  if((sChunk * nChunks) - length(fNames) > sChunk ) {
    nChunks = nChunks + 1
  }
  arqs2 <- NULL
  arqst <- list()
  
  foreach(j = 0:(nChunks - 1)) %do% {
    start <- j * sChunk + 1
    end <- (j + 1) * sChunk
    arqs <- mclapply(fNames[start:end], read.dbc, mc.cores = numCores)
    arqst <- append(arqst, arqs)
    rm(arqs)
  }
  if (rChunks != 0) {
    start <- nChunks * sChunk + 1
    end <- length(fNames)
    arqs <- mclapply(fNames[start:end], read.dbc, mc.cores = numCores)
    arqst <- append(arqst, arqs)
  }
  arqs2 <-
    foreach(I = 1:length(fNames)) %dopar% {
      if(is.data.frame(arqst[[I]])) {
        f1 <- arqst[[I]]
        estado <- substr(arquivos[I], 3, 4)
        ano <- substr(arquivos[I], 5, 6)
        mes <- substr(arquivos[I], 7, 8)
        ano <- as.integer(paste0("20", ano))
        f1 <-
          mutate(f1,
                 ESTADO = estado,
                 ANO = ano,
                 MES = mes)
      }
      else {
        next
      }
    }
  X <- data.table::rbindlist(arqs2, fill=TRUE) 
  rm(arqst)
  rm(arqs2)
  return(X)
}


converteDBCtoPQ <- function(Prefixo, Ano, Diretorio, UFpar, MESESpar, DirPQs) {
  # Prefixo:  o tipo de APAC a converter (PA, AR, AQ, AM, AN, AB, etc)
  # Ano: em formato de string, dois digitos ("08", "12", etc)
  # Diretorio: caminho até os arquivos *.dbc
  # UFpar: qual estado a converter
  # MESESpar: quais meses a converter; default são todos os meses.
  # esta funcao faz a leitura dos arquivos *.dbc de uma determinada UF para os
  # MESES determinados por uma máscara do tipo (XX|XX|XX|XX|XX|XX) onde XX é 
  # uma string representando cada mês (dois dígitos).
  # DirPQs: diretorio onde serão gravados os arquivos Parquet (caminho relativo dentro
  # do "Diretorio")
  # A função retorna um data.frame que pode então ser salvo em CSV no diretório apropriado
  
  # a função original que convertia os .dbc para data.frame estava consumindo os
  # recursos da máquina e, por conseguinte, não conseguia lidar com situações
  # onde os arquivos *.dbc (de um ano) ficavam com tamanhos muito grandes.
  # para contornar isso, vou fazer a conversão individual e gravar no arquivo
  # Parquet no estilo de stream; ou seja, sem paralelismo.
  # vou ter perda de desempenho, mas vai ser possível converter todas as PAs.
  
  # converteDBCtoPQ(PREF,ANO,diretorioBase,UF,MESES,diretorioPQs)
  # Diretorio <- diretorioBase
  # Prefixo <- PREF
  # UFpar <- "MG"
  # Ano <- "16"
  # MESESpar <- MESES
  # DirPQs <- diretorioPQs
  arquivos <- list.files(Diretorio, paste0(Prefixo,UFpar,Ano,MESESpar,".*.dbc"))
  
  fNames <- lapply(arquivos, function(x) {file.path(Diretorio,x)})
  
  fSizes <- file.size(arquivos)
  
  logfunc(paste0("Starting the processing of ", length(fNames), " files; UF: ", UFpar, " Ano:", Ano))
  
  i <- 1
  start <- 1
  end <- 1
  soma <- 0
  indices <- c()
  for (tam in fSizes) {
    if ((soma + tam) > MEMLIMIT*(2**20)) { # maior que 500M
      end <- i - 1
      indices <- append(indices, c(start,end))
      #print(c(start,end))
      start <- i 
      soma <- tam
    } else {
      soma <- soma + tam
      #print(soma)
      if (i == length(fSizes)) { # ultimo elemento
        indices <- append(indices, c(start,i))
        #print(c(start,i))
      }
    }
    i <- i + 1
  }
  if (indices[length(indices)] < length(fNames)) {
    start <- indices[length(indices)] + 1
    end <- length(fNames)
    indices <- append(indices, c(start,end))
  }

  start <- NULL
  end <- NULL
  rm(start)
  rm(end)
  
  
  i <- 1
  arqpq <- 0
  while(TRUE) {
   if (i+1 <= length(indices)) {
     fNameslote <- fNames[indices[i]:indices[(i+1)]]
     farquivos <- stringr::str_remove(fNameslote, paste0(Diretorio))
     farquivos <- stringr::str_remove(farquivos,"/")
     i <- i + 2
   } else {
     
     break;
   }

    # tamanho do Chunk igual ao número de cores
    sChunk <- numCores
    N <- length(fNameslote)
  
    if(length(fNameslote) > sChunk) {
      nChunks <- floor(length(fNameslote)/sChunk)
    } else {
      nChunks <- 1
      sChunk <- length(fNameslote)
    }
    rChunks <- length(fNameslote) %% sChunk
    if((sChunk * nChunks) - length(fNameslote) > sChunk ) {
      nChunks = nChunks + 1
    }
    arqs2 <- NULL
    arqst <- list()
  
    foreach(j = 0:(nChunks - 1)) %do% {
      start <- j * sChunk + 1
      end <- (j + 1) * sChunk
      if (!testcfg) {
        arqs <- mclapply(fNameslote[start:end], read.dbc, mc.cores = numCores)
        arqst <- append(arqst, arqs)
        logfunc(paste0("Reading ", length(fNameslote[start:end]), " DBC files: "))
      } else {
        print(paste0("Start: ",start, " end: ", end))
        for (faq in fNameslote[start:end]) {
          logfunc(paste0("Testing reading ", faq, " file"))
        }
      }

      
      rm(arqs)
    }
    if (rChunks != 0) {
      start <- nChunks * sChunk + 1
      end <- length(fNameslote)
      if (!testcfg) {
        arqs <- mclapply(fNameslote[start:end], read.dbc, mc.cores = numCores)
        arqst <- append(arqst, arqs)
        logfunc(paste0("Reading the last ", length(fNameslote[start:end]), " DBC files: "))
      } else {
        print(paste0("Start: ",start, " end: ", end))
        for (faq in fNameslote[start:end]) {
          logfunc(paste0("Testing reading last ", faq, " file"))
        }
      }
    }
    if (!testcfg) {
      arqs2 <-
        foreach(I = 1:length(fNameslote)) %dopar% {
          if(is.data.frame(arqst[[I]])) {
            f1 <- arqst[[I]]
            estado <- substr(farquivos[I], 3, 4)
            ano <- substr(farquivos[I], 5, 6)
            mes <- substr(farquivos[I], 7, 8)
            ano <- as.integer(paste0("20", ano))
            f1 <-
              mutate(f1,
                    ESTADO = estado,
                    ANO = ano,
                    MES = mes)
          }
          else {
            next
          }
        }
      logfunc(paste0(length(fNameslote)," files processed; creating the dataframe"))
      X <- data.table::rbindlist(arqs2, fill=TRUE) 
      rm(arqst)
      rm(arqs2)
    } else {
      logfunc(paste0("Testing: ", length(fNameslote), " would be processed and dataframe created."))
    }
    
    caminhohive <- paste0("ESTADO=",UFpar,"/ANO=",paste0("20",Ano))
    system(paste0("mkdir -p ", diretorioDest, "/",caminhohive))
    nomearquivopq <- paste0(nomearqPQPrefixo,arqpq,".parquet")
    if (!testcfg) {
      write_parquet(X, file.path(diretorioDest,caminhohive,nomearquivopq))
      logfunc(paste0("Parquet file: ", paste0(caminhohive,"/", nomearquivopq), " created."))
    } else {
      logfunc(paste0("Parquet file ",paste0(caminhohive,"/", nomearquivopq), " would be created."))
    }
    
    arquivosInfo <- file.info(unlist(fNameslote), extra_cols = FALSE)
    mtimes <- arquivosInfo[,"mtime"]
    ultimdata <- max(mtimes)
    mtimestr <- strftime(ultimdata, tz = "UTC", format = "%Y%m%d%H%M")
    if (!testcfg) {
      system(paste0("touch -m -t ", mtimestr, " ", file.path(diretorioDest,caminhohive,nomearquivopq)))
    }
    arqpq <- arqpq + 1
    rm(X)
  }

}



