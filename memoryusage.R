PREF = "PA"
IDarqMG <- c("MG2201a","MG2201b","MG2203b","MG2204b","MG2203a","MG2301b","MG2302b","MG2310a","MG2306a","MG1501","MG1502","MG1509","MG1506","MG1512","MG0803","MG0809","MG0808", "AL0802","AL0801","AL0804","AL0811","AL1011","AL1607","AL2008","AL2012","AL2002","MA1804","MA2002","MA1907","MA2203","MA2207","MA2405")
arqnames <- paste0(PREF,IDarqMG,".dbc")
library(read.dbc)
tamarquivos <- file.size(arqnames)
tammemoria <- c()
for (arq in arqnames) {
     f1 <- read.dbc(arq)
     tammemoria <- append(tammemoria,object.size(f1))
}
df <- data.frame(x = tamarquivos, y = tammemoria)
saveRDS(df,"dadosMemoriaArquivos.RDS")
library(tidyverse)
library(ggthemes)
ggplot(df) + geom_point(aes(x=x/(2**20),y=y/(2**20))) + 
  labs(x ="Tamanho Arquivos (Mb)", y = "Tam. Objeto Memoria (Mb)") +
  theme_clean()
fit <- lm(y ~ x, data = df)
summary(fit)
confint(fit)
