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

  # conecta no servidor MLFlow e retorna um objeto client MLFlow
  mlflow_track_uri <- get_MlFlow_Url()
  mlflow::mlflow_set_tracking_uri(mlflow_track_uri)
  client_obj <- mlflow::mlflow_client()
  print(mlflow::mlflow_get_tracking_uri())


  # coleta os experimentos do MLFlow e verifica se o nome do experimento
  # enviado por parametro já existe.
  exp_id <- checkExperiment(experiment_name = experiment_name,
                            mlflow_client = client_obj)


  # cria uma nova rodada
  id = as.integer(exp_id[1,1])
  with(run_obj <-mlflow_start_run(experiment_id = id), {
    print("run Forest!!!")
  })

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
save_artifact <- function(run_obj, df_artifact=NULL, file_path_artifact=NULL){

  library(mlflow)
  library(readr)
  library(uuid)

  if (missing(file_path_artifact)==FALSE)  {
    system("mlflow --version")
    command <- paste("mlflow artifacts log-artifact","--local-file",
                     file_path_artifact, "--run-id", run_obj$run_uuid,
                     sep=" ")
    system(command)
  }

  if (missing(df_artifact)==FALSE)  {
    system("mlflow --version")
    uuid_str = UUIDgenerate(use.time=TRUE, n=1)
    df_path = paste("/tmp/",uuid_str,".csv", sep="")
    write_csv(df_artifact, df_path)
    command <- paste("mlflow artifacts log-artifact","--local-file", df_path,
                     "--run-id", run_obj$run_uuid,
                     sep=" ")
    system(command)
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

  # retona objeto contendo credenciais do MLFlow
  mlflow_track_uri <- get_MlFlow_Url()
  mlflow::mlflow_set_tracking_uri(mlflow_track_uri)
  client_obj <- mlflow::mlflow_client()
  print(mlflow::mlflow_get_tracking_uri())

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

  mlflow::mlflow_end_run(
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
checkExperiment <- function(experiment_name, mlflow_client){
  
  print(experiment_name)
  exp_id <- NULL
  try(
      exp_id <- mlflow::mlflow_get_experiment(name=experiment_name)
  )
  print(exp_id)

  if (is.null(exp_id)) {
      print("Creating a new experiment...")
      expId <- mlflow::mlflow_create_experiment(experiment_name,
        client = mlflow_client
      )
      print(expId)
  } else {
    print("MLFlow experiment already exists..")
    if (exp_id[1,4]=="deleted") {
      print("The experiment exists but cannot be used because was already deleted")
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



#' Retorna a URL do MLFlow de acordo com a variavel de ambiente MLFLOW_URI
#' Caso contrario retorna um valor padrao.
#'
#' @return string contendo a URL do MLFlow utilizada pela lib para integracao
get_MlFlow_Url <- function(){
  uri <- Sys.getenv("MLFLOW_URI")
  if (uri=="") {
    uri = "http://192.168.7.234:5000"
  }
  return(uri)
}
