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
