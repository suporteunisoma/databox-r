
#' Inicia publicacao no MLFlow
#'
#' Tipicamente esta funcao deve ser posicionada no inicio do script R para
#' que o MLFlow possa medir o tempo de execução do modelo.
#'
#' @param experiment_name nome unico do experimento MLFlow
#' @return id da rodada necessario para finalizacao da publicacao no MLFlow
#' @export
start_mlflow <- function(experiment_name){

  library(mlflow)

  # valores utilizados nos testes...
  #experiment_name <- "meu_teste_do_R"

  # conecta no servidor MLFlow e retorna um objeto client MLFlow
  client_obj <- mlflow_client(tracking_uri = "http://192.168.7.234:5000/")

  # coleta os experimentos do MLFlow e verifica se o nome do experimento
  # enviado por parametro já existe.
  exp_id <- checkExperiment(experiment_name = experiment_name, client = client_obj)

  # se o experimento ainda nao existir, cria um novo registro
  if (exp_id==0) {
    mlflow_create_experiment(name=experiment_name, client = client_obj)
  }

  # cria uma nova rodada
  run_obj = mlflow_start_run(
    experiment_id = exp_id,
    client = client_obj
  )

  return (run_obj);
}

#' Salva artefato no repositorio do MLFLow
#'
#' Esta funcao envia para o MLFlow um artefato para o MLFlow relacionado
#' a uma rodada. Pode ser enviado um data frame ou mesmo o caminho de um
#' arquivo físico. No caso de ser um data frame, ele será convertido em CSV
#'
#' @param df_artfact DataFrame com dados a serem salvos como um artefato
#' @param file_path_artfact caminho do arquivo de artefato a ser salvo
#' @export
save_artifact <- function(run_obj, df_artfact=NULL, file_path_artfact=NULL){

  run_obj=d
  df_artfact=df
  file_path_artfact="/tmp/7bc1380e-fc5b-11eb-a0f6-0242ac130002.csv"

  library(mlflow)
  library(readr)
  library(uuid)

  # conecta no servidor MLFlow e retorna um objeto client MLFlow
  client_obj <- mlflow_client(tracking_uri = "http://192.168.7.234:5000/")

  if (missing(file_path_artfact)==FALSE)  {
    mlflow_log_artifact(file_path_artfact, run_id=run_obj$run_uuid, client=client_obj)
  }

  if (missing(df_artfact)==FALSE)  {
    uuid_str = UUIDgenerate(use.time=TRUE, n=1)
    df_path = paste("/tmp/",uuid_str,".csv", sep="")
    write_csv(df_artfact, df_path)
    mlflow_log_artifact(df_path, run_id=run_obj$run_uuid, client=client_obj)
    file.remove(df_path) # limpa arquivos csv da pasta temporaria
  }

}


#' Sinaliza MLFlow de que o processamento acabou. Opcionalmente pode ser
#' utilizado também pra salvar metricas e parametros.
#'
#'
#' @param run_obj id da rodada recebido no retorno da funcao start_mlflow
#' @param df_parameter DataFrame de tags contendo as colunas: params e values
#' @param df_metric DataFrame de metricas contendo: params e values
#' @param final_status string com status final do MLFlow. Use MLFlowStatusEnum
#' @export
finish_mlflow <- function(run_obj, df_parameter=NULL, df_metric=NULL, final_status){

  library(mlflow)

  # conecta no servidor MLFlow e retorna um objeto client MLFlow
  client_obj <- mlflow_client(tracking_uri = "http://192.168.7.234:5000/")

  # salva a lista de parametros do modelo
  if (missing(df_parameter)==FALSE)  {
    for(i in 1:nrow(df_parameter)) {
      row <- df_parameter[i,]
      mlflow_log_param(row$params, row$values, run_id=run_obj$run_uuid, client=client_obj)
    }
  }

  # salva a lista de metricas do modelo
  if (missing(df_metric)==FALSE)  {
    for(i in 1:nrow(df_metric)) {
      row <- df_metric[i,]
      mlflow_log_metric(row$params, row$values, run_id=run_obj$run_uuid, client=client_obj)
    }
  }

  mlflow_end_run(
    status = final_status,
    run_id = run_obj$run_uuid,
    client = client_obj
  )

}


#' Coleta os experimentos do MLFlow e verifica se o nome do experimento
#' enviado por parametro já existe.
#'
#' @param experiment_name nome unico do experimento MLFlow
#' @param client objeto contendo o MLFlow client
#' @return id do experimento do MLFlow ou 0 caso nao exista
checkExperiment <- function(experiment_name, client){

  exper_list <- mlflow_list_experiments(
    view_type = c("ACTIVE_ONLY", "DELETED_ONLY", "ALL"),
    client = client
  )

  exp_id = 0
  for(i in 1:nrow(exper_list)) {
    row <- exper_list[i,]
    if (row$name==experiment_name) {
      exp_id = row$experiment_id
    }
  }

  return(exp_id)
}


#' Funcao que emula um enumerador com os valores esperados pelo MLFlow para
#' definicao dos status finais de uma rodada. Utilize o exemplo abaixo como
#' um dos argumentos da funcao finish_mlflow caso queira definir que a rodada
#' finalizou com sucesso:
#' MLFlowStatusEnum()$FINISHED
#'
#' @return string contendo um dos status esperados pelo MLFlow
#' @export
MLFlowStatusEnum <- function() {
  list(FINISHED = "FINISHED", FAILED = "FAILED", KILLED = "KILLED")
}
