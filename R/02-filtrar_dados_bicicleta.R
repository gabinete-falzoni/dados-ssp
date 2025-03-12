library('tidyverse')
library('tidylog')
library('sf')
library('readxl')
library('arrow')


# Estrutura de pastas
pasta_dados <- '../dados'
# pasta_dados <- '/media/livre/REDRUM/gitlab/dados-ssp/dados/'


# ------------------------------------------------------------------------------
# Filtrar bases 2016-2021 - Furtos e roubos de bicicletas
# ------------------------------------------------------------------------------

ssp_old <- sprintf('%s/dados_ssp_2016-2021.parquet', pasta_dados)
ssp_old <- open_dataset(ssp_old)
# schema(ssp_old)

ssp_old <- ssp_old %>%
  # filter(NOME_MUNICIPIO == 'S.PAULO') %>%
  filter(str_detect(DESCR_SUBTIPO_OBJETO, 'icicleta')) %>%
  select(
    ID_DELEGACIA, # Código da delegacia responsável pelo registro da ocorrencia
    NOME_DELEGACIA, # Delegacia responsável pelo registro
    ANO_BO, # Ano do BO
    NUM_BO, # Número do BO
    # VERSAO, # Versionamento do Registro
    DATA_OCORRENCIA_BO, # Data da Ocorrência
    HORA_OCORRENCIA_BO, # Hora da Ocorrência
    ANO = ANO_REGISTRO_BO, # Ano da Estatística
    MES = MES_REGISTRO_BO, # Mês da Estatística
    NOME_MUNICIPIO_CIRC, # Município da Delegacia de Circunscrição
    NOME_DELEGACIA_CIRC, # Delegacia de Circunscrição
    DESCRICAO_APRESENTACAO, # Corporação que apresentou a ocorrência
    DESCR_PERIODO, # Período da Ocorrência
    AUTORIA_BO = FLAG_AUTORIA_BO, # Conhecida / desconhecida
    FLAG_FLAGRANTE, # Indica se houve flagrante (S= sim; N=não)
    FLAG_STATUS, # Indica se é crime consumado ou tentado
    # DESC_LEI, # Legislação Penal
    RUBRICA, # Natureza juridica da ocorrencia
    DESCR_CONDUTA, # Tipo de local ou circunstancia que qualifica a ocorrencia
    DESDOBRAMENTO, # Desdobramentos juridicos envolvidos na ocorrencia
    # CIRCUNSTANCIA, # Situação auxiliar da natureza criminal
    DESCR_TIPOLOCAL, # Descreve grupo de tipos de locais onde se deu o fato
    DESCR_SUBTIPOLOCAL, # Descreve subgrupo de tipos de locais, vinculado ao tipo de local,  onde se deu o fato
    BAIRRO, # Bairro da Ocorrência
    # LOGRADOURO_VERSAO, # Versionamento do Logradouro do BO
    LOGRADOURO, # Logradouro dos fatos
    NUMERO_LOGRADOURO, # Numero do Logradouro dos fatos
    CEP,
    LATITUDE,
    LONGITUDE,
    DESCR_MODO_OBJETO, # Situação do objeto na ocorrencia
    DESCR_TIPO_OBJETO, # Tipo de objeto - grande categoria
    DESCR_SUBTIPO_OBJETO, # Subtipo de objeto
    CONT_OBJETO,
    QUANTIDADE_OBJETO, # Contagem do objeto
    # MARCA_OBJETO # Marca/Modelo do objeto,
  ) %>%
  collect()

# Criar ID para tratar das duplicatas
ssp_old <- ssp_old %>%
  mutate(ID_BO = str_c(ID_DELEGACIA, ANO_BO, NUM_BO, sep = '-'), .before = 1)

# Remover duplicatas
# TODO: CHecar qual coluna não selecionada estaria diferente na base para não remover o distinct()
ssp_old <- ssp_old %>% distinct()

# Considerando somente NOME_MUNICIPIO == 'S.PAULO'
ssp_old %>% group_by(RUBRICA) %>% tally()
# RUBRICA                                     n
# <chr>                                   <int>
# 1 A.I.-Furto (art. 155)                     492
# 2 A.I.-Furto de coisa comum (art. 156)        1
# 3 A.I.-Furto qualificado (art. 155, §4o.)   139
# 4 A.I.-Roubo (art. 157)                     269
# 5 Furto (art. 155)                        75164
# 6 Furto de coisa comum (art. 156)            39
# 7 Furto qualificado (art. 155, §4o.)      23258
# 8 Roubo (art. 157)                        15401

ssp_old %>% group_by(RUBRICA) %>% summarise(n = n_distinct(ID_BO))
# RUBRICA                                     n
# <chr>                                   <int>
# 1 A.I.-Furto (art. 155)                     469
# 2 A.I.-Furto de coisa comum (art. 156)        1
# 3 A.I.-Furto qualificado (art. 155, §4o.)   115
# 4 A.I.-Roubo (art. 157)                     241
# 5 Furto (art. 155)                        71731
# 6 Furto de coisa comum (art. 156)            38
# 7 Furto qualificado (art. 155, §4o.)      20774
# 8 Roubo (art. 157)                        14471

n_distinct(ssp_old$ID_BO)
# [1] 107646

# Agrupar rubricas para simplificação
ssp_old <- ssp_old %>%
  mutate(RUBRICA_REV = case_when(str_detect(RUBRICA, 'Furto qualificado') ~ 'Furto qualificado',
                                 str_detect(RUBRICA, 'Furto de coisa comum') ~ 'Furto',
                                 str_detect(RUBRICA, 'Furto \\(art\\. 155\\)') ~ 'Furto',
                                 str_detect(RUBRICA, 'Roubo') ~ 'Roubo',
                                 TRUE ~ RUBRICA),
         .after = 'RUBRICA')

ssp_old %>% count(RUBRICA_REV)
# RUBRICA_REV           n
# <chr>             <int>
# 1 Furto             75696
# 2 Furto qualificado 23397
# 3 Roubo             15670

ssp_old %>% group_by(RUBRICA_REV) %>% summarise(n = n_distinct(ID_BO))
ssp_old %>% distinct() %>% group_by(RUBRICA_REV) %>% summarise(n = n_distinct(ID_BO))
# RUBRICA_REV           n
# <chr>             <int>
# 1 Furto             72221
# 2 Furto qualificado 20868
# 3 Roubo             14652


# ssp_old %>% filter(ANO == 2021 & RUBRICA_REV == 'Furto') %>% select(DESCR_CONDUTA) %>% distinct()
# filter: removed 32,236 rows (83%), 6,602 rows remaining
# select: dropped 30 variables (ID_BO, ID_DELEGACIA, NOME_DELEGACIA, ANO_BO, NUM_BO, …)
# distinct: removed 6,586 rows (>99%), 16 rows remaining
# A tibble: 16 × 1
#   DESCR_CONDUTA
#   <chr>
# 1 RESIDENCIA
# 2 JOALHERIA
# 3 OUTROS
# 4 CONDOMINIO RESIDENCIAL
# 5 INTERIOR ESTABELECIMENTO
# 6 INTERIOR DE VEICULO
# 7 ESTABELECIMENTO COMERCIAL
# 8 TRANSEUNTE
# 9 ESTABELECIMENTO-OUTROS
#10 VEICULO
#11 CONDOMINIO COMERCIAL
#12 ESTABELECIMENTO ENSINO
#13 INTERIOR TRANSPORTE COLETIVO
#14 CARGA
#15 VEÍCULO COMPARTILHADO
#16 FIOS E CABOS

# ------------------------------------------------------------------------------
# Filtrar bases 2022-2024 - Furtos e roubos de bicicletas
# ------------------------------------------------------------------------------

ssp <- sprintf('%s/dados_ssp_2022-2024.parquet', pasta_dados)
ssp <- open_dataset(ssp)
# schema(ssp)

ssp <- ssp %>%
  # filter(NOME_MUNICIPIO == 'S.PAULO') %>%
  filter(str_detect(DESCR_SUBTIPO_OBJETO, 'icicleta')) %>%
  select(
    ID_DELEGACIA, # Código da delegacia responsável pelo registro da ocorrencia
    NOME_DELEGACIA, # Delegacia responsável pelo registro
    ANO_BO, # Ano do BO
    NUM_BO, # Número do BO
    VERSAO, # Versionamento do Registro ### Não tem na base anterior
    DATA_OCORRENCIA_BO, # Data da Ocorrência
    HORA_OCORRENCIA_BO = HORA_OCORRENCIA, # Hora da Ocorrência
    ANO, # Ano da Estatística
    MES, # Mês da Estatística,
    NOME_MUNICIPIO_CIRC, # Município da Delegacia de Circunscrição
    NOME_DELEGACIA_CIRC, # Delegacia de Circunscrição
    DESCRICAO_APRESENTACAO, # Corporação que apresentou a ocorrência
    DESCR_PERIODO, # Período da Ocorrência
    AUTORIA_BO, # Conhecida / desconhecida
    FLAG_FLAGRANTE, # Indica se houve flagrante (S= sim; N=não)
    FLAG_STATUS, # Indica se é crime consumado ou tentado
    DESC_LEI, # Legislação Penal ### Não tem na base anterior
    RUBRICA, # Natureza juridica da ocorrencia
    DESCR_CONDUTA, # Tipo de local ou circunstancia que qualifica a ocorrencia
    DESDOBRAMENTO, # Desdobramentos juridicos envolvidos na ocorrencia
    CIRCUNSTANCIA, # Situação auxiliar da natureza criminal ### Não tem na base anterior
    DESCR_TIPOLOCAL, # Descreve grupo de tipos de locais onde se deu o fato
    DESCR_SUBTIPOLOCAL, # Descreve subgrupo de tipos de locais, vinculado ao tipo de local,  onde se deu o fato
    BAIRRO, # Bairro da Ocorrência
    LOGRADOURO_VERSAO, # Versionamento do Logradouro do BO ### Não tem na base anterior
    LOGRADOURO, # Logradouro dos fatos
    NUMERO_LOGRADOURO, # Numero do Logradouro dos fatos
    CEP,
    LATITUDE,
    LONGITUDE,
    DESCR_MODO_OBJETO, # Situação do objeto na ocorrencia
    DESCR_TIPO_OBJETO, # Tipo de objeto - grande categoria
    DESCR_SUBTIPO_OBJETO, # Subtipo de objeto
    CONT_OBJETO,
    QUANTIDADE_OBJETO, # Contagem do objeto
    MARCA_OBJETO # Marca/Modelo do objeto, ### Não tem na base anterior
  ) %>%
  collect()

# Criar ID para tratar das duplicatas
ssp <- ssp %>%
  mutate(ID_BO = str_c(ID_DELEGACIA, ANO_BO, NUM_BO, sep = '-'), .before = 1)

# Remover duplicatas
# TODO: CHecar qual coluna não selecionada estaria diferente na base para não remover o distinct()
ssp <- ssp %>% distinct()


# Manter somente as rubricas de Furto (art. 155); Furto qualificado (art. 155, §4o.);
# Furto de coisa comum (art. 156); Roubo (art. 157); e Apropriação indébita (art. 168)
ssp <- ssp %>%
  # Filtro pelo artigo, por causa das diferentes redações...
  filter(str_detect(RUBRICA, '1[56][5678]')) %>%
  # ...mas remover Extorsão
  filter(!str_detect(RUBRICA, 'Extorsão'))


ssp %>% group_by(RUBRICA) %>% tally()
# RUBRICA                                     n
# <chr>                                   <int>
# 1 A.I.-Furto (art. 155)                      39
# 2 A.I.-Furto de coisa comum (art. 156)        1
# 3 A.I.-Furto qualificado (art. 155, §4o.)     6
# 4 A.I.-Roubo (art. 157)                      23
# 5 Apropriação indébita (art. 168)           414
# 6 Furto (art. 155)                        46704
# 7 Furto de coisa comum (art. 156)            38
# 8 Furto qualificado (art. 155, §4o.)        858
# 9 Roubo (art. 157)                         6887

ssp %>% group_by(RUBRICA) %>% summarise(n = n_distinct(ID_BO))
# RUBRICA                                     n
# <chr>                                   <int>
# 1 A.I.-Furto (art. 155)                      36
# 2 A.I.-Furto de coisa comum (art. 156)        1
# 3 A.I.-Furto qualificado (art. 155, §4o.)     4
# 4 A.I.-Roubo (art. 157)                      21
# 5 Apropriação indébita (art. 168)           407
# 6 Furto (art. 155)                        42412
# 7 Furto de coisa comum (art. 156)            31
# 8 Furto qualificado (art. 155, §4o.)        733
# 9 Roubo (art. 157)                         5967


# Agrupar rubricas para simplificação
ssp <- ssp %>%
  mutate(RUBRICA_REV = case_when(str_detect(RUBRICA, 'Furto qualificado') ~ 'Furto qualificado',
                                 str_detect(RUBRICA, 'Furto de coisa comum') ~ 'Furto',
                                 str_detect(RUBRICA, 'Furto \\(art\\. 155\\)') ~ 'Furto',
                                 str_detect(RUBRICA, 'Roubo') ~ 'Roubo',
                                 TRUE ~ RUBRICA),
         .after = 'RUBRICA')

ssp %>% count(RUBRICA_REV)
# RUBRICA_REV                         n
# <chr>                           <int>
# 1 Apropriação indébita (art. 168)   414
# 2 Furto                           46782
# 3 Furto qualificado                 864
# 4 Roubo                            6910


ssp %>% group_by(RUBRICA_REV) %>% summarise(n = n_distinct(ID_BO))
# RUBRICA_REV                         n
# <chr>                           <int>
# 1 Apropriação indébita (art. 168)   407
# 2 Furto                           42479
# 3 Furto qualificado                 737
# 4 Roubo                            5981


# ------------------------------------------------------------------------------
# Juntar bases: 2016-2024
# ------------------------------------------------------------------------------

# Colunas que estão no df1 mas não no df2 para rbind()
c(setdiff(names(ssp_old), names(ssp)))
c(setdiff(names(ssp), names(ssp_old)))

# Inserir em df1 colunas do df2 que não existem no df1, como NA
ssp_old[setdiff(names(ssp), names(ssp_old))] <- NA


# A base de 2021 vai, na verdade, até uma parte de 2022 - remover duplicatas,
# priorizando a versão mais atual, do site da SSP
ssp_old <- ssp_old %>% filter(!ID_BO %in% ssp$ID_BO)


# Juntar tudo
ssp_out <- rbind(ssp_old, ssp)


# ------------------------------------------------------------------------------
# Limpezas adicionais
# ------------------------------------------------------------------------------

ssp_out %>% select(DESCR_SUBTIPO_OBJETO) %>% distinct()
# 1 Bicicleta
# 2 Porta-bicicleta
# 3 Bicicleta Elétrica
# 4 Roda de Bicicleta

# Excluir porta-bicicleta e roda de bicicleta
ssp_out <- ssp_out %>%
  filter(DESCR_SUBTIPO_OBJETO == 'Bicicleta' | DESCR_SUBTIPO_OBJETO == 'Bicicleta Elétrica')


check_this <- function(df, group_col) {
          this <- df
          this %>% group_by(!!group_col) %>% count() %>% head(20) %>% print()
        }

check_this(ssp_out, expr(RUBRICA_REV))

ssp_out <- ssp_out %>%
  mutate(FLAG_STATUS = toupper(FLAG_STATUS),
         RUBRICA_REV = ifelse(RUBRICA_REV == 'Apropriação indébita (art. 168)', 'Apropriação indébita', RUBRICA_REV),
         DESCR_PERIODO = toupper(DESCR_PERIODO),
         DESCR_MODO_OBJETO = toupper(DESCR_MODO_OBJETO),
         DESCR_CONDUTA = toupper(DESCR_CONDUTA),
         DESCR_CONDUTA = ifelse(DESCR_CONDUTA == 'NULL', as.character(NA), DESCR_CONDUTA),
         DESCR_PERIODO = ifelse(DESCR_PERIODO == 'NULL', as.character(NA), DESCR_PERIODO),
         DESCR_TIPOLOCAL = toupper(DESCR_TIPOLOCAL),
         DESCR_SUBTIPOLOCAL = toupper(DESCR_SUBTIPOLOCAL),
         DESCR_TIPOLOCAL = ifelse(DESCR_TIPOLOCAL == 'CONDOMINIO COMERCIAL', 'CONDOMÍNIO COMERCIAL', DESCR_TIPOLOCAL),
         DESCR_TIPOLOCAL = ifelse(DESCR_TIPOLOCAL == 'CONDOMINIO RESIDENCIAL', 'CONDOMÍNIO RESIDENCIAL', DESCR_TIPOLOCAL),
         DESCR_TIPOLOCAL = ifelse(DESCR_TIPOLOCAL == 'CENTRO COMERC./EMPRESARIAL', 'CENTRO COMERCIAL/EMPRESARIAL', DESCR_TIPOLOCAL),
         DESCRICAO_APRESENTACAO = ifelse(DESCRICAO_APRESENTACAO == 'NULL', as.character(NA), DESCRICAO_APRESENTACAO),
         DESCRICAO_APRESENTACAO = ifelse(DESCRICAO_APRESENTACAO == 'Por outros', 'Por Outros', DESCRICAO_APRESENTACAO)

)

# check_cols <- c('ANO_BO', 'ANO', 'DESCRICAO_APRESENTACAO', 'DESCR_PERIODO',
#                 'AUTORIA_BO', 'FLAG_FLAGRANTE', 'FLAG_STATUS', 'RUBRICA_REV',
#                 'DESCR_CONDUTA', 'DESDOBRAMENTO', 'DESCR_TIPOLOCAL', 'DESCR_SUBTIPOLOCAL',
#                 'BAIRRO', 'DESCR_MODO_OBJETO', 'DESCR_TIPO_OBJETO', 'DESCR_SUBTIPO_OBJETO',
#                 'QUANTIDADE_OBJETO', 'VERSAO', 'DESC_LEI', 'CIRCUNSTANCIA',
#                 'LOGRADOURO_VERSAO')
#
# check_this <- function(df, group_col) {
#   this <- df
#   this %>% group_by(!!group_col) %>% count() %>% print()
# }
#
#
# for (col in check_cols) {
#   check_this(ssp_out, col)
#   # ssp_out %>% group_by(!!col) %>% tally() %>% print()
# }

# ssp_out %>% group_by(ANO) %>% summarise(n = n_distinct(ID_BO))

ssp_out %>%
  mutate(ANO_OCORRENCIA = year(DATA_OCORRENCIA_BO)) %>%
  filter(NOME_MUNICIPIO_CIRC == 'S.PAULO' & ANO_OCORRENCIA > 2015) %>%
  group_by(RUBRICA_REV2, ANO_OCORRENCIA) %>%
  summarise(n = n_distinct(ID_BO))  %>%
  head(20)


# Exportar resultados
out_file <- sprintf('%s/dados_ssp_2016-2024_bicicletas.csv', pasta_dados)
write_delim(ssp_out, out_file, delim = ';')


ssp_out %>%
  mutate(ANO_OCORRENCIA = year(DATA_OCORRENCIA_BO)) %>%
  mutate(RUBRICA_REV2 = ifelse(RUBRICA_REV == 'Furto qualificado', 'Furto', RUBRICA_REV)) %>%
  mutate(RUBRICA_REV2 = ifelse(str_detect(RUBRICA_REV2, 'Apropriação indébita'), 'Furto', RUBRICA_REV2)) %>%
  filter(NOME_MUNICIPIO_CIRC == 'S.PAULO' & ANO_OCORRENCIA > 2015) %>%
  group_by(RUBRICA_REV2, ANO_OCORRENCIA) %>%
  summarise(n = n_distinct(ID_BO)) %>%
  head(20)


shape_sp <- ssp_out %>%
  mutate(ANO_OCORRENCIA = year(DATA_OCORRENCIA_BO)) %>%
  mutate(RUBRICA_REV2 = ifelse(RUBRICA_REV == 'Furto qualificado', 'Furto', RUBRICA_REV)) %>%
  mutate(RUBRICA_REV2 = ifelse(str_detect(RUBRICA_REV2, 'Apropriação indébita'), 'Furto', RUBRICA_REV2)) %>%
  filter(NOME_MUNICIPIO_CIRC == 'S.PAULO' & ANO_OCORRENCIA > 2015) %>%
  filter(!is.na(LATITUDE) & LATITUDE != 0) %>%
  group_by(ID_BO) %>%
  filter(row_number() == 1) %>%
  ungroup() %>%
  # group_by(QUANTIDADE_OBJETO) %>% tally() %>% tail()
  select(ID_BO,
         ANO_OCORRENCIA,
         DATA_OCORRENCIA_BO, # Data da Ocorrência
         NOME_DELEGACIA_CIRC, # Delegacia de Circunscrição
         RUBRICA, # Natureza juridica da ocorrencia
         RUBRICA_REV, # Natureza juridica da ocorrencia
         RUBRICA_REV2, # Natureza juridica da ocorrencia
         BAIRRO, # Bairro da Ocorrência
         LOGRADOURO_VERSAO, # Versionamento do Logradouro do BO ### Não tem na base anterior
         LOGRADOURO, # Logradouro dos fatos
         NUMERO_LOGRADOURO, # Numero do Logradouro dos fatos
         CEP,
         LATITUDE,
         LONGITUDE,
         DESCR_SUBTIPO_OBJETO, # Subtipo de objeto
         CONT_OBJETO,
         QUANTIDADE_OBJETO, # Contagem do objeto
         MARCA_OBJETO) %>%
  # select(LATITUDE, LONGITUDE) %>%
  st_as_sf(coords = c('LONGITUDE', 'LATITUDE'), crs = 4326)

out_file <- sprintf('%s/dados_ssp_bicicleta_sp.gpkg', pasta_dados)
st_write(shape_sp, out_file, driver = 'GPKG', append = FALSE)


# # Ignorar esta coluna
# ssp %>% select(DESCR_MODO_OBJETO) %>% distinct()
# # 1 SUBTRAÍDO
# # 2 Subtraído
#
# # Ignorar esta coluna
# ssp %>% select(DESCR_TIPO_OBJETO) %>% distinct()
# # <chr>               <int>
# # 1 Esporte e Lazer      6627
# # 2 Esporte e lazer     11115
# # 3 Meios de Transporte   396
#
# # Ignorar esta coluna
# ssp %>% select(FLAG_STATUS) %>% distinct()
# # 1 CONSUMADO
#
#
# ssp %>% select(RUBRICA) %>% arrange(RUBRICA) %>% distinct() %>% slice(40:51)
# # A.I.-Furto (art. 155)
# # A.I.-Furto qualificado (art. 155, §4o.)
# # Ameaça (art. 147)
# # Apreensão de Adolescente
# # Apropriação de coisa achada (art. 169, par. único, II)
# # Apropriação de coisa havida por erro, caso fortuito, força da natureza (art 169)
# # Apropriação indébita (art. 168)
# # Associação Criminosa (art. 288)
# # Atropelamento
# # Captura de procurado
# # Caput Corromper ou facilitar a corrupção de menor de 18 anos (244B)
# # Colisão
# # Comunicação falsa de crime ou contravenção (art. 340)
# # Dano (art. 163)
# # Descumprimento de medida protetiva de urgência (art. 24-A)
# # Desobediência (art. 330)
# # Difamação (art. 139)
# # Dirigir sem Permissão ou Habilitação (Art. 309)
# # Disparo de arma de fogo (Art. 15)
# # Disparo de arma de fogo (art. 28)
# # Drogas sem autorização ou em desacordo (Art.33, caput)
# # Entrega de objeto localizado/apreendido
# # Entrega de veículo localizado/apreendido
# # Estelionato (art. 171)
# # Estupro (art. 213)
# # Exercício arbitrário das próprias razões (art. 345)
# # Extorsão (art. 158)
# # Fuga de local de acidente (Art. 305)
# # Furto (art. 155)
# # Furto de coisa comum (art. 156)
# # Furto qualificado (art. 155, §4o.)
# # Homicídio (art. 121)
# # Incêndio (art. 250, caput)
# # Injúria (art. 140)
# # Lesão corporal (art. 129)
# # Lesão corporal culposa (art. 129. §6o.)
# # Lesão corporal culposa na direção de veículo automotor (Art. 303)
# # Localização/Apreensão de objeto
# # Localização/Apreensão de veículo
# # Localização/Apreensão e Entrega de objeto
# # Localização/Apreensão e Entrega de objeto
# # Localização/Apreensão e Entrega de veículo
# # Omissão cautela na guarda/condução animais (art. 31)
# # Outros não criminal
# # Perda/Extravio
# # Posse ou porte ilegal de arma de fogo de uso restrito (Art. 16)
# # Queda acidental
# # Receptação (art. 180)
# # Roubo (art. 157)
# # Seqüestro e cárcere privado (art. 148)
# # Vias de fato (art. 21)
# # Violência Doméstica
#
# ssp %>%
#   select(DESC_LEI) %>%
#   mutate(DESC_LEI = str_replace(DESC_LEI, '\\.', '')) %>%
#   arrange(DESC_LEI) %>%
#   distinct()
# # 1 Acidente de trânsito                      # Sai
# # 2 Ato infracional                           # Fica
# # 3 Código Penal
# # 4 DL 3688/41 - Contravenções Penais
# # 5 L 10826/03 - Estatuto do Desarmamento
# # 6 L 11340/06 - Violência Doméstica
# # 7 L 11343/06 - Entorpecentes
# # 8 L 8069/90 - ECA
# # 9 L 9503/97 - Código de Trânsito Brasileiro
# # 10 Localização e/ou Devolução
# # 11 Não Criminal
# # 12 Outros - não criminal
# # 13 Perda/Extravio
# # 14 Pessoa
# # 15 Título I - Pessoa (arts 121 a 154)
# # 16 Título II - Patrimônio (arts 155 a 183)   # Fica
# # 17 Título VI - Costumes (arts 213 a 234)     # Sai
#
# ssp %>%
#   mutate(DESC_LEI = str_replace(DESC_LEI, '\\.', '')) %>%
#   filter(DESC_LEI == 'Ato infracional') %>%
#   select(DESCR_SUBTIPO_OBJETO,
#          DESCR_TIPO_OBJETO, RUBRICA, DESCR_CONDUTA,
#          DESDOBRAMENTO, CIRCUNSTANCIA, ANO) %>%
#   head(20)

