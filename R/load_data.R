#' Carrega dados de um dataset
#'
#' Esta funcao carrega um dataset mapeados no Catalogo de Dados da plataforma
#' UniSoma e retorna para a funcao R na forma de uma estrutura de dados
#' apropriada.
#'
#' @param entity_catalog id of data catalog entity
#' @return dados coletada da base fonte segundo o catalogo
#' @export
load_data <- function(entity_catalog){

  access_token <- dbx_authenticate()
  dados <- dbx_call_service(access_token = access_token, entity = entity_catalog)
  source <- dados$entity_source

  if (is.na(source)) {
    print(paste(entity_catalog, "nao encontrado no catalogo", sep=" "))

  } else {

    #para cada tipo de 'source' acessa os dados de diferentes formas...
    if (source=="JDBC_SOURCE") {
      result = load_jdbc_data(dados$entity_args, entity_catalog)
    } else if (source=="CSV_URL_SOURCE") {
      result = load_csv_url_data(dados$entity_args, entity_catalog)
    }

    return(result)
  }
}


load_jdbc_data <- function(jdbc_args, entity_catalog){
  library(stringr)

  jdbc_driver <- ""
  jdbc_host <- ""
  jdbc_user <- ""
  jdbc_pwd <- ""
  jdbc_driver_path <- ""
  jdbc_sql <- ""

  for (i in 1:nrow(jdbc_args)) {
    arg_name <- jdbc_args[i,1]
    arg_value <- jdbc_args[i,2]

    if (arg_name=="JDBC_DRIVER") {
      jdbc_driver <- arg_value

      if (str_detect(jdbc_driver, "sqlite")) {
        jdbc_driver_path = "lib/sqlite-jdbc-3.34.0.jar"
      } else if (str_detect(jdbc_driver, "postgresql")){
        jdbc_driver_path = "lib/postgresql.jar"
      } else if (str_detect(jdbc_driver, "jtds")) {
        jdbc_driver_path = "lib/jtds.jar"
      }

    } else if (arg_name=="JDBC_HOST") {
      jdbc_host <- arg_value
    } else if (arg_name=="JDBC_PWD") {
      jdbc_pwd <- arg_value
    } else if (arg_name=="JDBC_USER") {
      jdbc_user <- arg_value
    } else if (arg_name=="JDBC_SQL") {
      jdbc_sql <- arg_value
    }
  }

  drv <- RJDBC::JDBC(jdbc_driver, jdbc_driver_path)
  conn <- RJDBC::dbConnect(drv, jdbc_host, jdbc_user, jdbc_pwd)

  # roda a consulta
  result_set <- dbGetQuery(conn, paste(jdbc_sql, sep=" "))

  return(result_set)
}


load_csv_url_data <- function(csv_args, entity_catalog){

  csv_url <- ""
  csv_sep <- ""
  csv_dec_sep <- ""

  for (i in 1:ncol(csv_args)) {
    arg_name <- csv_args[1,i]
    arg_value <- csv_args[2,i]

    if (arg_name=="CSV_URL") {
      csv_url <- arg_value
    } else if (arg_name=="CSV_SEPARATOR") {
      csv_sep <- arg_value
    } else if (arg_name=="CSV_DECIMAL_SEP") {
      csv_dec_sep <- arg_value
    }
  }

  df <- read.csv2(csv_url, sep=csv_sep, dec='.')
  return(df)
}

dbx_call_service <- function(access_token, entity) {
  library(httr)
  library(jsonlite)
  auth <- paste("Bearer", access_token,sep=' ')
  res <- POST(paste("http://192.168.0.25:8080/databox/api/metadata/findMetaDataArgsByCatalog/",entity, sep=''),
              add_headers(Authorization = auth, "accept"="application/json", "content-type"="application/json"))
  if (res$status_code==200) {
    json_content <- rawToChar(res$content)
    ret <- fromJSON(json_content)
    cols <- Reduce('+', lengths(ret$argName))
    mtx <- matrix(rbind(ret$argName, ret$argValue), ncol=cols)

    source <- ret$sourceEngine$source$name[1]

    # retorna dois objetos: (a) os argumentos para se conectar na fonte dos dados
    # e (b) o tipo da fonte de dados
    return(list(entity_args = mtx, entity_source = source))

  } else {
    return (NULL)
  }

}


dbx_authenticate <- function() {
  library(httr)
  library(jsonlite)
  access_token <- ""

  login <- list(
    grant_type = "password",
    username = "unisoma",
    password = "unisoma"
  )
  auth <- "Basic SUQtQzIzMjMzQUEtQUI2Ri00REI0LUE2NEQtQjVFRDI0Nzk2NDJBOiRjZDY3OWYyMDczNGM0NzUxOGQ1NTQ1MTgwNjNlMTRkZg=="
  res <- POST("http://192.168.7.221:8080/authorization/oauth/token",
              add_headers(Authorization = auth ),
              body = login)
  if (res$status_code==200) {
    json_content <- rawToChar(res$content)
    access_token <- fromJSON(json_content)$access_token
  }
  return(access_token)
}
