library('tidyverse')
library('tidylog')
library('readxl')
library('arrow')


# Estrutura de pastas
pasta_dados <- '/home/flavio/Dados/gitlab/dados-ssp/dados'

ssp <- sprintf('%s/dados_ssp_2022-2024.parquet', pasta_dados)
ssp <- open_dataset(ssp)
# schema(ssp)

ssp <- ssp %>%
  filter(NOME_MUNICIPIO == 'S.PAULO') %>%
  filter(str_detect(DESCR_SUBTIPO_OBJETO, 'icicleta')) %>%
  select(
    ID_DELEGACIA, # Código da delegacia responsável pelo registro da ocorrencia
    NOME_DELEGACIA, # Delegacia responsável pelo registro
    ANO_BO, # Ano do BO
    NUM_BO, # Número do BO
    VERSAO, # Versionamento do Registro
    DATA_OCORRENCIA_BO, # Data da Ocorrência
    HORA_OCORRENCIA, # Hora da Ocorrência
    ANO, # Ano da Estatística
    MES, # Mês da Estatística
    NOME_DELEGACIA_CIRC, # Delegacia de Circunscrição
    DESCRICAO_APRESENTACAO, # Corporação que apresentou a ocorrência
    DESCR_PERIODO, # Período da Ocorrência
    AUTORIA_BO, # Conhecida / desconhecida
    FLAG_FLAGRANTE, # Indica se houve flagrante (S= sim; N=não)
    FLAG_STATUS, # Indica se é crime consumado ou tentado
    DESC_LEI, # Legislação Penal
    RUBRICA, # Natureza juridica da ocorrencia
    DESCR_CONDUTA, # Tipo de local ou circunstancia que qualifica a ocorrencia
    DESDOBRAMENTO, # Desdobramentos juridicos envolvidos na ocorrencia
    CIRCUNSTANCIA, # Situação auxiliar da natureza criminal
    DESCR_TIPOLOCAL, # Descreve grupo de tipos de locais onde se deu o fato
    DESCR_SUBTIPOLOCAL, # Descreve subgrupo de tipos de locais, vinculado ao tipo de local,  onde se deu o fato
    BAIRRO, # Bairro da Ocorrência
    LOGRADOURO_VERSAO, # Versionamento do Logradouro do BO
    LOGRADOURO, # Logradouro dos fatos
    NUMERO_LOGRADOURO, # Numero do Logradouro dos fatos
    LATITUDE,
    LONGITUDE,
    DESCR_MODO_OBJETO, # Situação do objeto na ocorrencia
    DESCR_TIPO_OBJETO, # Tipo de objeto - grande categoria
    DESCR_SUBTIPO_OBJETO, # Subtipo de objeto
    QUANTIDADE_OBJETO, # Contagem do objeto
    MARCA_OBJETO # Marca/Modelo do objeto,
  ) %>%
  collect()

ssp %>% select(DESCR_SUBTIPO_OBJETO) %>% distinct()
# 1 Bicicleta
# 2 Porta-bicicleta
# 3 Bicicleta Elétrica
# 4 Roda de Bicicleta

ssp %>% select(DESCR_MODO_OBJETO) %>% distinct()
# 1 SUBTRAÍDO
# 2 Subtraído

ssp %>% select(DESCR_TIPO_OBJETO) %>% distinct()
# 1 Esporte e lazer
# 2 Peça automotiva/acessório
# 3 Esporte e Lazer
# 4 Meios de Transporte

ssp %>% select(FLAG_STATUS) %>% distinct()
# 1 CONSUMADO

ssp %>% select(RUBRICA) %>% arrange(RUBRICA) %>% distinct() %>% slice(40:51)
# A.I.-Furto (art. 155)
# A.I.-Furto qualificado (art. 155, §4o.)
# Ameaça (art. 147)
# Apreensão de Adolescente
# Apropriação de coisa achada (art. 169, par. único, II)
# Apropriação de coisa havida por erro, caso fortuito, força da natureza (art 169)
# Apropriação indébita (art. 168)
# Associação Criminosa (art. 288)
# Atropelamento
# Captura de procurado
# Caput Corromper ou facilitar a corrupção de menor de 18 anos (244B)
# Colisão
# Comunicação falsa de crime ou contravenção (art. 340)
# Dano (art. 163)
# Descumprimento de medida protetiva de urgência (art. 24-A)
# Desobediência (art. 330)
# Difamação (art. 139)
# Dirigir sem Permissão ou Habilitação (Art. 309)
# Disparo de arma de fogo (Art. 15)
# Disparo de arma de fogo (art. 28)
# Drogas sem autorização ou em desacordo (Art.33, caput)
# Entrega de objeto localizado/apreendido
# Entrega de veículo localizado/apreendido
# Estelionato (art. 171)
# Estupro (art. 213)
# Exercício arbitrário das próprias razões (art. 345)
# Extorsão (art. 158)
# Fuga de local de acidente (Art. 305)
# Furto (art. 155)
# Furto de coisa comum (art. 156)
# Furto qualificado (art. 155, §4o.)
# Homicídio (art. 121)
# Incêndio (art. 250, caput)
# Injúria (art. 140)
# Lesão corporal (art. 129)
# Lesão corporal culposa (art. 129. §6o.)
# Lesão corporal culposa na direção de veículo automotor (Art. 303)
# Localização/Apreensão de objeto
# Localização/Apreensão de veículo
# Localização/Apreensão e Entrega de objeto
# Localização/Apreensão e Entrega de objeto
# Localização/Apreensão e Entrega de veículo
# Omissão cautela na guarda/condução animais (art. 31)
# Outros não criminal
# Perda/Extravio
# Posse ou porte ilegal de arma de fogo de uso restrito (Art. 16)
# Queda acidental
# Receptação (art. 180)
# Roubo (art. 157)
# Seqüestro e cárcere privado (art. 148)
# Vias de fato (art. 21)
# Violência Doméstica

ssp %>%
  select(DESC_LEI) %>%
  mutate(DESC_LEI = str_replace(DESC_LEI, '\\.', '')) %>%
  arrange(DESC_LEI) %>%
  distinct()
# 1 Acidente de trânsito                      # Sai
# 2 Ato infracional                           # Fica
# 3 Código Penal
# 4 DL 3688/41 - Contravenções Penais
# 5 L 10826/03 - Estatuto do Desarmamento
# 6 L 11340/06 - Violência Doméstica
# 7 L 11343/06 - Entorpecentes
# 8 L 8069/90 - ECA
# 9 L 9503/97 - Código de Trânsito Brasileiro
# 10 Localização e/ou Devolução
# 11 Não Criminal
# 12 Outros - não criminal
# 13 Perda/Extravio
# 14 Pessoa
# 15 Título I - Pessoa (arts 121 a 154)
# 16 Título II - Patrimônio (arts 155 a 183)   # Fica
# 17 Título VI - Costumes (arts 213 a 234)     # Sai

ssp %>%
  mutate(DESC_LEI = str_replace(DESC_LEI, '\\.', '')) %>%
  filter(DESC_LEI == 'Acidente de trânsito') %>%
  select(DESCR_SUBTIPO_OBJETO, DESCR_TIPO_OBJETO, RUBRICA, DESCR_CONDUTA,
         DESDOBRAMENTO, CIRCUNSTANCIA, ANO) %>%
  head(20)

ssp %>%
  mutate(DESC_LEI = str_replace(DESC_LEI, '\\.', '')) %>%
  filter(DESC_LEI == 'Título II - Patrimônio (arts 155 a 183)') %>%
  select(DESCR_SUBTIPO_OBJETO, # DESCR_TIPO_OBJETO,
         RUBRICA, DESCR_CONDUTA,
         DESDOBRAMENTO, CIRCUNSTANCIA, ANO) %>%
  sample_n(20)

ssp %>%
  filter(RUBRICA == 'Dirigir sem Permissão ou Habilitação (Art. 309)') %>%
  select(DESCR_SUBTIPO_OBJETO, DESC_LEI,
         RUBRICA, DESCR_CONDUTA,
         DESDOBRAMENTO, CIRCUNSTANCIA, ANO) %>%
  sample_n(20)


ssp <- ssp %>%
  mutate(DESC_LEI = str_replace(DESC_LEI, '\\.', '')) %>%
  filter(DESC_LEI != 'Acidente de trânsito')
