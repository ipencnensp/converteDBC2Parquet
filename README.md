# Conversão de arquivos DBC do DATASUS para Parquet e CSV.

Estes scripts funcionam da seguinte forma:

1. Parte-se do princípio que os arquivos `*.dbc` já estão salvos na máquina, ou seja, já foi feito o download.  
   Para o download, especialmente se for em máquina Linux, recomenda-se fortemente o uso do utilitário `wget` com opções de
   mirror, por exemplo, assim:
   * `wget -m -np -nH -nd ftp://ftp.datasus.gov.br/dissemin/publicos/SIASUS/200801_/Dados/PA*`
   * esta maneira de chamar o `wget` vai manter os _timestamps_ dos arquivos baixados iguais aos originais do site do DATASUS, não vai ascender aos diretórios superiores (`-np`), não vai criar diretórios com o nome da máquina (`-nH`) e também não vai criar os diretórios do caminho completo dos arquivos (`-nd`).
   * é importante, ao se fazer o download dos arquivos *.dbc, que se mantenha os _timestamps_ originais, já que o script de conversão para Parquet não vai refazer os arquivos que são mais velhos do que o Parquet.
   * a ideia é que se possa rodar periodicamente o comando `wget` para cada tipo de arquivo a ser converido, já que o DATASUS faz atualizações retroativas nos arquivos `*.dbc`; com isso, se algum arquivo `*.dbc` foi alterado, quando se rodar o script de conversão `conversaoDBCparaParquet.R`, ele vai detectar que o `*.dbc` é mais recente do que o Parquet correspondente e vai recriar esse Parquet específico, ou então, vai criar um novo, se for esse o caso.
   * a recriação ou criação do arquivo Parquet é para cada ano, esse é o esquema de particionamento escolhido, ou seja, todos os arquivos `*.dbc` de cada ano serão convertidos para um ou mais arquivos Parquet no caminho _Hive_ `ESTADO=UF/ANO=YYYY` no diretório de destino dos arquivos Parquets.
   * no arquivo de configuração, `config.R` você poderá escolher apagar (default) os arquivos Parquet pré-existentes para determinado ano, ou então, renomeá-los (uma vez apenas), para o padrão "-old.parquet"

2. O script foi projetado para converter separadamente os diversos arquivos das APACS do SIASUS, por isso, no arquivo de configuração `config.R` você vai indicar o prefixo das APACS que deseja converter, bem como o diretório resultante para os arquivos Parquet.

3. Antes de chamar o script de conversão, você deve criar o seu arquivo de configuração `config.R`; um exemplo é fornecido, que deve ser renomeado para `config.R` depois de preenchido corretamente com os parâmetros para a conversão.

4. Preferencialmente, chame o script de dentro do diretório onde estão os arquivos `*.dbc` a serem convertidos, utilize sempre o programa `Rscript` do **R** para rodar o script, da seguinte forma:
  * `Rscript CAMINHO/PARA/SCRIPTS/conversaoDBCparaParquet.R > logconversao.txt 2>&1 &`
  * nesse comando, estamos redirecionando a saída para um arquivo de log para poder inspecionar se deu tudo certo; também será redirecionada a saída de erro padrão para o mesmo arquivo, e a execução será em segundo plano (background).
  * Pode-se omitir o redirecionamento:  
  `Rscript CAMINHO/PARA/SCRIPTS/conversaoDBCparaParquet.R`
  * nesse caso, toda a saída será mostrada no próprio console/terminal da execução.
  
5. Um último script foi adicionado para permitir converter os arquivos `*.dbc` em `CSV`. O seu funcionamento é o mesmo do script de conversão para Parquet.
  
### Bibliotecas necessárias

- `Arrow`
- `read.dbc` instalada do [github](https://github.com/danicat/read.dbc)
- `dplyr`
- `data.table`
- `stringr`
- `foreign`
- `parallel`
- `doParallel`

