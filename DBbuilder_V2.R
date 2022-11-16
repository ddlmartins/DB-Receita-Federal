library(sqldf)
library(tidyverse)
library(vroom)
library(RSQLite)
library(readxl)
library(rvest)
memory.limit(size=56000)

setwd(dirname(rstudioapi::getActiveDocumentContext()$path))

#=== DOWNLOAD dos originais ====
x = read_html("https://www.gov.br/receitafederal/pt-br/assuntos/orientacao-tributaria/cadastros/consultas/dados-publicos-cnpj") %>% 
    html_elements(xpath="//*[@id='parent-fieldname-text']/p//a") %>% 
    html_attr('href') %>% .[matches(".zip", vars = .)]

map(x, ~download.file(., destfile = paste0("../Originais/2022/Outubro/", basename(.))))

#=== MONTAR A BASE ====

drv <- dbDriver("SQLite") 
con <- dbConnect(drv, "RF_RJ_10_10_2022.db") 

#=== ESTABELECIMENTOS ====
files <-  list.files("../Originais/2022/Outubro", pattern =  "Estabelecimento", full.names = T)

map(files, function(x){
  print(x)
  if(x==first(files)){
  dbWriteTable(con, "ESTABELECIMENTOS", vroom(x, col_names = F, col_types = c(.default = "c")) %>% 
                                                  `colnames<-`(c("CNPJ_RADICAL",
                                                                 "CNPJ_ORDEM",
                                                                 "CNPJ_DV",
                                                                 "MATRIZ_FILIAL",
                                                                 "NOME_FANTASIA",
                                                                 "SITUACAO_CADASTRAL",
                                                                 "DATA_SITUACAO_CADASTRAL",
                                                                 "MOTIVO_SITUACAO_CADASTRAL",
                                                                 "CIDADE_EXTERIOR",
                                                                 "PAIS_EXTERIOR",
                                                                 "DATA_INICIO_ATIVIDADE",
                                                                 "CNAE_PRINCIPAL",
                                                                 "CNAE_SECUNDARIA",
                                                                 "TIPO_LOGRADOURO",
                                                                 "LOGRADOURO",
                                                                 "NUMERO",
                                                                 "COMPLEMENTO",
                                                                 "BAIRRO",
                                                                 "CEP",
                                                                 "UF",
                                                                 "COD_MUNICIPIO",
                                                                 "DDD1",
                                                                 "TEL1",
                                                                 "DDD2",
                                                                 "TEL2",
                                                                 "DDD_FAX",
                                                                 "FAX",
                                                                 "E_MAIL",
                                                                 "SITUACAO_ESPECIAL",
                                                                 "DATA_SITUACAO_ESPECIAL")) %>% filter(UF=="RJ"))
  } else {
    dbAppendTable(con, "ESTABELECIMENTOS", vroom(x, col_names = F, col_types = c(.default = "c")) %>% 
                   `colnames<-`(c("CNPJ_RADICAL",
                                  "CNPJ_ORDEM",
                                  "CNPJ_DV",
                                  "MATRIZ_FILIAL",
                                  "NOME_FANTASIA",
                                  "SITUACAO_CADASTRAL",
                                  "DATA_SITUACAO_CADASTRAL",
                                  "MOTIVO_SITUACAO_CADASTRAL",
                                  "CIDADE_EXTERIOR",
                                  "PAIS_EXTERIOR",
                                  "DATA_INICIO_ATIVIDADE",
                                  "CNAE_PRINCIPAL",
                                  "CNAE_SECUNDARIA",
                                  "TIPO_LOGRADOURO",
                                  "LOGRADOURO",
                                  "NUMERO",
                                  "COMPLEMENTO",
                                  "BAIRRO",
                                  "CEP",
                                  "UF",
                                  "COD_MUNICIPIO",
                                  "DDD1",
                                  "TEL1",
                                  "DDD2",
                                  "TEL2",
                                  "DDD_FAX",
                                  "FAX",
                                  "E_MAIL",
                                  "SITUACAO_ESPECIAL",
                                  "DATA_SITUACAO_ESPECIAL")) %>% filter(UF=="RJ"))
  }})

#=== EMPRESAS ====
files <- list.files("../Originais/2022/Outubro", pattern = "Empresa", full.names = T )

map(files, function(x){
  print(x)
  if(x==first(files)){
  dbCreateTable(con, "EMPRESAS", vroom(x, col_names = F, col_types = c(.default = "c")) %>% 
                                                `colnames<-`(c("CNPJ_RADICAL",
                                                      "RAZAO_SOCIAL",
                                                      "NATUREZA_JURIDICA",
                                                      "QUALIFICACAO_RESPONSAVEL",
                                                      "CAPITAL_SOCIAL",
                                                      "PORTE_EMPRESA",
                                                      "ENTE_FEDERATIVO")))

  } else {
    dbAppendTable(con, "EMPRESAS",  vroom(x, col_names = F, col_types = c(.default = "c")) %>% 
                                             `colnames<-`(c("CNPJ_RADICAL",
                                                            "RAZAO_SOCIAL",
                                                            "NATUREZA_JURIDICA",
                                                            "QUALIFICACAO_RESPONSAVEL",
                                                            "CAPITAL_SOCIAL",
                                                            "PORTE_EMPRESA",
                                                            "ENTE_FEDERATIVO")))
  }})

#=== SÓCIOS ====
files <- list.files("../Originais/2022/Outubro", pattern =  "Socio", full.names = T)

map(files, function(x){
  print(x)
  if(x==first(files)){
    dbCreateTable(con, "SOCIOS", vroom(x, col_names = F, col_types = c(.default = "c")) %>% 
                                            `colnames<-`(c("CNPJ_RADICAL",
                                                     "IDENTIFICADOR_SOCIO",
                                                     "NOME_SOCIO",
                                                     "CNPJ_CPF_SOCIO",
                                                     "QUALIFICACAO_SOCIO",
                                                     "DATA_ENTRADA_SOCIEDADE",
                                                     "PAIS_ESTRANGEIRO",
                                                     "REPRESENTANTE_LEGAL",
                                                     "NOME_REPRESENTANTE",
                                                     "QUALIFICACAO_REPRESENTANTE",
                                                     "FAIXA_ETARIA_SOCIO")) %>% 
                              mutate_all(~iconv(.,from = "latin1", to = "UTF-8")))
  } else {
    dbAppendTable(con, "SOCIOS",  vroom(x, col_names = F, col_types = c(.default = "c")) %>% 
                                           `colnames<-`(c("CNPJ_RADICAL",
                                                          "IDENTIFICADOR_SOCIO",
                                                          "NOME_SOCIO",
                                                          "CNPJ_CPF_SOCIO",
                                                          "QUALIFICACAO_SOCIO",
                                                          "DATA_ENTRADA_SOCIEDADE",
                                                          "PAIS_ESTRANGEIRO",
                                                          "REPRESENTANTE_LEGAL",
                                                          "NOME_REPRESENTANTE",
                                                          "QUALIFICACAO_REPRESENTANTE",
                                                          "FAIXA_ETARIA_SOCIO")) %>% 
                    mutate_all(~iconv(.,from = "latin1", to = "UTF-8")))
  }})
#=== SIMPLES ====
files <- list.files("../Originais/2022/Outubro", pattern = "Simples", full.names = T)

dbWriteTable(con, "SIMPLES",  map_df(files, ~vroom(., col_names = F, col_types = c(.default = "c")) %>% 
                                            `colnames<-`(c("CNPJ_RADICAL",
                                                       "OP_SIMPLES",
                                                       "DATA_OP_SIMPLES",
                                                       "DATA_EX_SIMPLES",
                                                       "OP_MEI",
                                                       "DATA_OP_MEI",
                                                       "DATA_EX_MEI"))))


#=== JOIN ====

dbWriteTable(con, "BASE_RJ",  con %>% 
                              tbl("ESTABELECIMENTOS") %>%
                                  left_join(tbl(con, "EMPRESAS"), by="CNPJ_RADICAL") %>%
                                  left_join(tbl(con, "SIMPLES"), by="CNPJ_RADICAL") %>% 
                                  collect() %>% mutate_all(~iconv(.,from = "latin1", to = "UTF-8")))

dbRemoveTable(con, "ESTABELECIMENTOS")
dbRemoveTable(con, "EMPRESAS")
dbRemoveTable(con, "SIMPLES")

#teste = con %>% 
#        tbl("ESTABELECIMENTOS") %>%
#        left_join(tbl(con, "EMPRESAS")) %>%
#        left_join(tbl(con, "SOCIOS")) %>%
#        left_join(tbl(con, "SIMPLES")) %>%
#        head(10) %>% 
#        collect()

#teste = con %>% 
#        tbl("BASE_RJ") %>%
#        head(10) %>% 
#        collect()

dbDisconnect(con)
