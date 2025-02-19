library('tidyverse')
library('tidylog')
library('readxl')
library('arrow')


# Estrutura de pastas
pasta_dados <- '/home/flavio/Dados/gitlab/dados-ssp/dados'


# ------------------------------------------------------------------------------
# Transformar arquivos originais em .parquet -2016-2021 (só Bicicletas)
# ------------------------------------------------------------------------------

# Arquivos do pedido de LAI (extração em Setembro de 2022)
antigos <- sprintf('%s/Dados brutos 2016-2021.xlsx', pasta_dados)
antigos <- read_xlsx(antigos)
head(antigos)

out_file <- sprintf('%s/dados_ssp_2016-2021.parquet', pasta_dados)
write_parquet(antigos, out_file)


# ------------------------------------------------------------------------------
# Transformar arquivos originais em .parquet - 2022 - 2024 (todas ocorrências)
# ------------------------------------------------------------------------------

# Erros na base de dados estão sendo ignorados (ex. campo de data preenchido
# com caracteres que não fazem sentido como data)

# Arquivos 2022
ssp_2022 <- sprintf('%s/ObjetosSubtraidos_2022.xlsx', pasta_dados)
ssp_2022a <- read_xlsx(ssp_2022, sheet = 'OBJETOS_01TRI_2022', col_types = 'text')
ssp_2022b <- read_xlsx(ssp_2022, sheet = 'OBJETOS_02TRI_2022 ', col_types = 'text') # tem esse espaço
ssp_2022c <- read_xlsx(ssp_2022, sheet = 'OBJETOS_03TRI_2022', col_types = 'text')
ssp_2022d <- read_xlsx(ssp_2022, sheet = 'OBJETOS_04TRI_2022', col_types = 'text')

ssp_2022 <- rbind(ssp_2022a, ssp_2022b, ssp_2022c, ssp_2022d)
rm(ssp_2022a, ssp_2022b, ssp_2022c, ssp_2022d)


# Arquivos 2023
ssp_2023 <- sprintf('%s/ObjetosSubtraidos_2023.xlsx', pasta_dados)
ssp_2023a <- read_xlsx(ssp_2023, sheet = 'OBJETOS_01TRI_2023', col_types = 'text')
ssp_2023b <- read_xlsx(ssp_2023, sheet = 'OBJETOS_02TRI_2023', col_types = 'text')
ssp_2023c <- read_xlsx(ssp_2023, sheet = 'OBJETOS_03TRI_2023', col_types = 'text')
ssp_2023d <- read_xlsx(ssp_2023, sheet = 'OBJETOS_04TRI_2023', col_types = 'text')

ssp_2023 <- rbind(ssp_2023a, ssp_2023b, ssp_2023c, ssp_2023d)
rm(ssp_2023a, ssp_2023b, ssp_2023c, ssp_2023d)


# Arquivos 2024
ssp_2024 <- sprintf('%s/ObjetosSubtraidos_2024.xlsx', pasta_dados)
ssp_2024a <- read_xlsx(ssp_2024, sheet = 'OBJETOS_01TRI_2024', col_types = 'text')
ssp_2024b <- read_xlsx(ssp_2024, sheet = 'OBJETOS_02TRI_2024', col_types = 'text')
ssp_2024c <- read_xlsx(ssp_2024, sheet = 'OBJETOS_03TRI_2024', col_types = 'text')
ssp_2024d <- read_xlsx(ssp_2024, sheet = 'OBJETOS_04TRI_2024', col_types = 'text')

ssp_2024 <- rbind(ssp_2024a, ssp_2024b, ssp_2024c, ssp_2024d)
rm(ssp_2024a, ssp_2024b, ssp_2024c, ssp_2024d)


# Criar colunas em 2022 que existem em 2023 e 2024
ssp_2022 <- ssp_2022 %>% mutate(VERSAO = as.character(NA),
                                FLAG_ATO_INFRACIONAL = as.character(NA),
                                LOGRADOURO_VERSAO = as.character(NA),
                                FLAG_BLOQUEIO = as.character(NA),
                                FLAG_DESBLOQUEIO = as.character(NA))

# Juntar tudo
ssp <- rbind(ssp_2022, ssp_2023, ssp_2024)


ssp %>% select(HORA_OCORRENCIA) %>% sample_n(20) %>%
  mutate(HORA_OCORRENCIA = as.numeric(HORA_OCORRENCIA) * 24) %>%
  mutate(HORA_OCORRENCIA = as.Date(HORA_OCORRENCIA, origin = "1900-01-01"))

# Transformar colunas que não são string antes de exportar
ssp <-
  ssp %>%
  mutate(ANO_BO = as.integer(ANO_BO),
         # In Excel, 1900 is incorrectly treated as a leap year. This means Excel
         # counts February 29, 1900, which does not exist. Because of this, Excel's
         # date system is off by 2 days compared to the correct Gregorian calendar system.
         # In R, when using as.Date() with "1900-01-01" as the origin, it doesn't
         # account for this extra day in Excel's system. To fix this, you can
         # adjust the origin date by subtracting one day when converting.
         DATA_OCORRENCIA_BO = as.numeric(DATA_OCORRENCIA_BO),
         DATA_OCORRENCIA_BO = as.Date(DATA_OCORRENCIA_BO - 2, origin = "1900-01-01"),

         DATAHORA_REGISTRO_BO = as.numeric(DATAHORA_REGISTRO_BO),
         DATAHORA_REGISTRO_BO = as.Date(DATAHORA_REGISTRO_BO - 2, origin = "1900-01-01"),

         DATA_COMUNICACAO_BO = as.numeric(DATA_COMUNICACAO_BO),
         DATA_COMUNICACAO_BO = as.Date(DATA_COMUNICACAO_BO - 2, origin = "1900-01-01"),

         DATAHORA_IMPRESSAO_BO = as.numeric(DATAHORA_IMPRESSAO_BO),
         DATAHORA_IMPRESSAO_BO = as.Date(DATAHORA_IMPRESSAO_BO - 2, origin = "1900-01-01"),

         # Transformar string de fraction em horas - 1 represents a full day
         # (i.e., 24 hours). For example, 0.5 would represent 12:00 PM (half a day),
         # and 0.25 would represent 6:00 AM.
         HORA_OCORRENCIA = as.numeric(HORA_OCORRENCIA) * 24,
         HORA_OCORRENCIA = format(as.POSIXct(HORA_OCORRENCIA * 3600, origin = "1970-01-01", tz = "UTC"), "%H:%M:%S"),

         # Demais conversões de formato
         CONT_OBJETO = as.integer(CONT_OBJETO),
         QUANTIDADE_OBJETO = as.integer(QUANTIDADE_OBJETO),
         NUMERO_LOGRADOURO = as.integer(NUMERO_LOGRADOURO),
         ANO = as.integer(ANO),
         MES = as.integer(MES),

         LATITUDE  = as.double(str_replace(LATITUDE, ',', '.')),
         LONGITUDE = as.double(str_replace(LONGITUDE, ',', '.'))
  )

out_file <- sprintf('%s/dados_ssp_2022-2024.parquet', pasta_dados)
write_parquet(ssp, out_file)



ssp %>%
  filter(NOME_MUNICIPIO == 'S.PAULO') %>%
  filter(DESCR_SUBTIPO_OBJETO == 'Bicicleta') %>%
  filter(LATITUDE != '0') %>%
  mutate(LATITUDE = as.double(LATITUDE),
         LONGITUDE = as.double(LONGITUDE)) %>%
  select(47:51)
