
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
    dbx_driver = "org.postgresql.Driver"
    dbx_driver_path = "lib/postgresql.jar"
    dbx_driver_host = "jdbc:postgresql://192.168.7.221:5432/databox"
    dbx_driver_user = "postgres"
    dbx_driver_pwd = "databox"
  }
  drv <- RJDBC::JDBC(dbx_driver, dbx_driver_path)
  conn <- RJDBC::dbConnect(drv, dbx_driver_host, dbx_driver_user, dbx_driver_pwd)

  # obtem os metadados para acesso ao dado requisitado
  sql_args <- "select ea.arg_name, ea.arg_value, s.name as source
               from dbx_source_engine_arg ea
        inner join dbx_source_engine se on (ea.source_engine_id = se.id)
                       inner join dbx_catalog c on (c.source_engine_id = se.id)
                               inner join dbx_source s on (s.id = se.source_id)
              where c.entity=?"
  eargs <- dbGetQuery(conn, sql_args, entity_catalog)

  # obtem os metadados para acesso ao dado requisitado
  sql_defin <- "select cc.id, cc.name, cc.col_type, cc.is_not_null
                from dbx_catalog c
                inner join dbx_catalog_column cc on (cc.catalog_id = c.id)
                where c.entity=? and cc.is_active=1"
  edefn <- dbGetQuery(conn, sql_defin, entity_catalog)

  # retorna dois objetos: (a) os argumentos para se conectar na fonte dos dados
  # e (b) a definicao dos metadados da entidade.
  return(list(entity_args = eargs, entity_definition = edefn))

}

load_jdbc_data <- function(jdbc_args, entity_metadata, entity_catalog){
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

load_csv_url_data <- function(csv_args, entity_metadata, entity_catalog){

  csv_url <- ""
  csv_sep <- ""
  csv_dec_sep <- ""

  for (i in 1:nrow(csv_args)) {
    arg_name <- csv_args[i,1]
    arg_value <- csv_args[i,2]

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
