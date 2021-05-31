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
}


load_meta_data <- function(entity_catalog) {
  library(DBI)
  library(rJava)
  library(RJDBC)

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
  result <- dbGetQuery(conn, "select * from catalog where entity=?", entity_catalog)
}


load_data_from_SVN <- function(path_repo, file_name){

  shellcmd <- paste("wget ", path_repo, file_name, sep = "")
  system(shellcmd)
  if (file.exists(file_name) ) {
    df <- read.csv2(file_name, sep=",", dec=".")
  } else {
    stop("Can not get copy of file")
  }

  df
}
