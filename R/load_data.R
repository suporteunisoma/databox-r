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

  #entity_catalog <- "bi_projeto"
  #faz autenticacao no DataBOX e coleta o SQL apropriado
  access_token <- dbx_authenticate()
  meta_data_sql <- dbx_call_service(access_token, entity_catalog)

  file <- "~/dump.Rds"
  if (file.exists(file)) {
    unlink(file)
  }

  #requisita do DataBOX os dados referentes ao Catalogo
  setwd("~/.local/")
  system(paste("/bin/python3", "~/databox-r/lib/load_data.py", paste("'",meta_data_sql, "'", sep=''), sep=' '),
      intern = FALSE, ignore.stdout = FALSE, ignore.stderr = FALSE, wait=FALSE
  )

  #retorna dados a partir de RDS volatil
  result <- readRDS(file)

  return(result)
}



dbx_call_service <- function(access_token, entity_catalog) {
  library(httr)
  library(jsonlite)

  auth <- paste("Bearer", access_token, sep=' ')
  res <- POST(paste("http://192.168.7.221:9091/databox/api/metadata/findMetaDataByCatalogName/", entity_catalog, sep=''),
              add_headers(Authorization = auth, "accept"="application/json", "content-type"="application/json"))
  if (res$status_code==200) {
    json_content <- rawToChar(res$content)
    ret <- fromJSON(json_content)
    sql <- ret$entityQuery
    return (sql)
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
  res <- POST("http://192.168.7.221:9092/authorization/oauth/token",
              add_headers(Authorization = auth ),
              body = login)
  if (res$status_code==200) {
    json_content <- rawToChar(res$content)
    access_token <- fromJSON(json_content)$access_token
  }
  return(access_token)
}
