#' Carrega dados de um dataset
#'
#' Esta funcao carrega um dataset mapeados no Catalogo de Dados da plataforma
#' UniSoma e retorna para a funcao R na forma de uma estrutura de dados
#' apropriada.
#'
#' @param entity_catalog id of data catalog entity
#' @return A matrix of the entity_catalog
#' @export
load_data <- function(entity_catalog){
  path <- paste("https://raw.githubusercontent.com/suporteunisoma/databox-r/c943edd1ad740d9199fd0a10dbbcb2a2b7038b9b/", entity_catalog, ".csv", sep="")
  df <- read.csv2(path, sep=",", dec=".")
  df

  entity_catalog = "bi_ginfra_helpdesk_ticket"
  result <- load_meta_data(entity_catalog)
  source <- result$entity_args[1,3]
  #para cada tipo de 'source' acessa os dados de diferentes maneiras
  if (source=="JDBC_SOURCE") {
    load_jdbc_data(result$entity_args, result$entity_definition, entity_catalog)
  }

}


load_meta_data <- function(entity_catalog) {
  library(DBI)
  library(rJava)
  library(RJDBC)
  library(tidyverse)

  dbx_driver <- Sys.getenv("DATA_BOX_JDBC_DRIVER")
  dbx_driver_path <- Sys.getenv("DATA_BOX_JDBC_LIB_PATH")
  dbx_driver_host <- Sys.getenv("DATA_BOX_JDBC_HOST")
  dbx_driver_user <- Sys.getenv("DATA_BOX_JDBC_USER")
  dbx_driver_pwd <- Sys.getenv("DATA_BOX_JDBC_PWD")

  if (dbx_driver=="") {
    dbx_driver = "org.sqlite.JDBC"
    dbx_driver_path = "/home/sqlite-jdbc-3.34.0.jar"
    dbx_driver_host = "jdbc:sqlite:/home/data_box.db"
    dbx_driver_user = "unisoma"
    dbx_driver_pwd = ""
  }
  drv <- RJDBC::JDBC(dbx_driver, dbx_driver_path)
  conn <- RJDBC::dbConnect(drv, dbx_driver_host, dbx_driver_user, dbx_driver_pwd)

  # obtem os metadados para acesso ao dado requisitado
  sql_args <- "select ea.arg_name, ea.arg_value, s.name as source
               from dbx_source_engine_arg ea
        inner join dbx_source_engine se on (ea.source_engine_id = se.source_id)
                       inner join dbx_catalog c on (c.source_engine_id = se.id)
                               inner join dbx_source s on (s.id = se.source_id)
              where c.entity=?"
  eargs <- dbGetQuery(conn, sql_args, entity_catalog)

  # obtem os metadados para acesso ao dado requisitado
  sql_defin <- "select cc.id, cc.name, cc.col_type, cc.is_not_null
                from dbx_catalog c
                inner join dbx_catalog_column cc on (cc.catalog_id = c.id)
                where c.entity=?"
  edefn <- dbGetQuery(conn, sql_defin, entity_catalog)

  # retorna dois objetos: (a) os argumentos para se conectar na fonte dos dados
  # e (b) a definicao dos metadados da entidade.
  return(list(entity_args = eargs, entity_definition = edefn))

}

load_jdbc_data <- function(jdbc_args, entity_metadata, entity_catalog){

  jdbc_driver = ""
  jdbc_host = ""
  jdbc_user = ""
  jdbc_pwd = ""
  jdbc_driver_path = ""

  for (i in 1:nrow(results)) {
    arg_name <- results[i,1]
    arg_value <- results[i,2]

    if (arg_name=="JDBC_DRIVER") {
      jdbc_driver <- arg_value

      #if (str_locate(jdbc_driver, "sqlite")[1,1]>0) {
      #   jdbc_driver_path = "/home/sqlite-jdbc-3.34.0.jar"
      #} else if (str_locate(jdbc_driver, "postgresql")[1,1]>0) {
         jdbc_driver_path = "/home/postgresql.jar"
      #}

    } else if (arg_name=="JDBC_HOST") {
      jdbc_host <- arg_value
    } else if (arg_name=="JDBC_PWD") {
      jdbc_pwd <- arg_value
    } else if (arg_name=="JDBC_USER") {
      jdbc_user <- arg_value
    }
  }

  print(jdbc_driver)
  print(jdbc_driver_path)
  print(jdbc_host)
  print(jdbc_user)
  print(jdbc_pwd)

  drv <- RJDBC::JDBC(jdbc_driver, jdbc_driver_path)
  conn <- RJDBC::dbConnect(drv, jdbc_host, jdbc_user, jdbc_pwd)

  print(conn)
}
