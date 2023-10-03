#' Carrega dados de um dataset
#'
#' Esta funcao carrega um dataset mapeados no Catalogo de Dados da plataforma
#' UniSoma e retorna para a funcao R na forma de uma estrutura de dados
#' apropriada.
#'
#' @param entity_catalog nome do item do catalogo
#' @return dados coletada da base fonte segundo o catalogo
#' @export
load_data <- function(entity_catalog){

  print("Removing dump file...")
  dumpFile <- "~/dump.Rds"
  if (file.exists(dumpFile)) {
    unlink(dumpFile)
  }

  print("Accessing DataBox...")
  #entity_catalog <- "bi_projeto"
  #faz autenticacao no DataBOX e coleta o SQL apropriado
  access_token <- dbx_authenticate()
  meta_data_sql <- dbx_call_service(access_token, entity_catalog)

  if (!is.null(meta_data_sql)) {

     #requisita do DataBOX os dados referentes ao Catalogo
     print("Collecting data...")
     setwd("~/.local/")
     system(paste("/bin/python3", "/opt/trino/load_data.py", paste("'",meta_data_sql, "'", sep=''), sep=' '),
         intern = TRUE, ignore.stdout = FALSE, ignore.stderr = FALSE
     )
     print("Convert RDS to Dataset...")
     #retorna dados a partir de RDS volatil
     result <- readRDS(dumpFile)
  } else {
    print("A consulta SQL nao foi encontrada ou o catalogo nao existe.")
  }

  return(result)
}




dbx_call_service <- function(access_token, entity_catalog) {
  library(httr)
  library(jsonlite)

  urlMetaData <- Sys.getenv("DATABOX_METADATA_URL")
  if (urlMetaData=="") {
    urlMetaData <- "http://192.168.7.221:9091/databox/api/dbxmetadata/findMetaDataByCatalogName/"
  }

  print("Call Dbx service...")
  auth <- paste("Bearer", access_token, sep=' ')
  res <- POST(paste(urlMetaData, entity_catalog, sep=''),
              add_headers(Authorization = auth, "accept"="application/json", "content-type"="application/json"))
  if (res$status_code==200) {
    json_content <- rawToChar(res$content)
    ret <- fromJSON(json_content)
    sql <- ret$entityQuery
    print("SQL clause extracted...")
    return (sql)
  } else {
    return (NULL)
  }

}


dbx_authenticate <- function() {
  library(httr)
  library(jsonlite)
  access_token <- ""

  urlAuth <- Sys.getenv("DATABOX_ACCOUNTS_URL")
  if (urlAuth=="") {
    urlAuth <- "http://192.168.7.221:9092/authorization/oauth/token"
  }
  auth <- Sys.getenv("DATABOX_BASIC_AUTH")
  if (auth=="") {
    auth <- "SUQtQzIzMjMzQUEtQUI2Ri00REI0LUE2NEQtQjVFRDI0Nzk2NDJBOiRjZDY3OWYyMDczNGM0NzUxOGQ1NTQ1MTgwNjNlMTRkZg=="
  }
  usrAuth <- Sys.getenv("DATABOX_BASIC_AUTH_USER")
  if (usrAuth=="") {
    usrAuth <- "unisoma"
  }
  usrPass <- Sys.getenv("DATABOX_BASIC_AUTH_PASS")
  if (usrPass=="") {
    usrPass <- "unisoma"
  }

  print("Dbx Authentication...")
  login <- list(
    grant_type = "password",
    username = usrAuth,
    password = usrPass
  )

  res <- POST(urlAuth,
              add_headers(Authorization = paste('Basic', auth, sep=' ') ),
              body = login)
  if (res$status_code==200) {
    json_content <- rawToChar(res$content)
    print("Access token extracted...")
    access_token <- fromJSON(json_content)$access_token
  }
  return(access_token)
}
