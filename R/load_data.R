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
  source("R/extractor.R", encoding="UTF8")

  md <- load_meta_data(entity_catalog)
  source <- md$entity_args[1,3]

  #para cada tipo de 'source' acessa os dados de diferentes maneiras
  if (source=="JDBC_SOURCE") {
    result = load_jdbc_data(md$entity_args, md$entity_definition, entity_catalog)
  } else if (source=="CSV_URL_SOURCE") {
    result = load_csv_url_data(md$entity_args, md$entity_definition, entity_catalog)
  }

  return(result)
}
