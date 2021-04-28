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
  path <- paste("https://raw.githubusercontent.com/suporteunisoma/databox-r/main/", entity_catalog, ".csv", sep="")
  df <- read.csv2(path, sep=",", dec=".")
  df
}
