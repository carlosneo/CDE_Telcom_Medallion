# Setup Automático

## Objetivo

Este repositório Git hospeda a automação de configuração para o HOL. O script consiste em uma série de comandos CDE CLI que primeiro criam dados sintéticos no armazenamento em nuvem para cada participante e depois criam o Recurso CDE compartilhado por cada participante.

Esta automação não executa o material real do laboratório. Estes são fáceis de criar através da interface CDE e não exigem automação.

## Índice

* [Requisitos](https://github.com/pdefusco/CDE_121_HOL/blob/main/step_by_step_guides/portuguese/part_00_setup.md#requisitos)
* [Informações Importantes](https://github.com/pdefusco/CDE_121_HOL/blob/main/step_by_step_guides/portuguese/part_00_setup.md#informações-importantes)
* [Opções de Implantação](https://github.com/pdefusco/CDE_121_HOL/blob/main/step_by_step_guides/portuguese/part_00_setup.md#opções-de-implantação)
  * [1. Instruções de Implantação com Docker](https://github.com/pdefusco/CDE_121_HOL/blob/main/step_by_step_guides/portuguese/part_00_setup.md#1-instruções-de-implantação-com-docker)
  * [2. Instruções de Implantação Local](https://github.com/pdefusco/CDE_121_HOL/blob/main/step_by_step_guides/portuguese/part_00_setup.md#2-instruções-de-implantação-local)
* [Instruções de Desmontagem](https://github.com/pdefusco/CDE_121_HOL/blob/main/step_by_step_guides/portuguese/part_00_setup.md#instruções-de-desmontagem)
* [Resumo](https://github.com/pdefusco/CDE_121_HOL/blob/main/step_by_step_guides/portuguese/part_00_setup.md#resumo)

## Requisitos

Para implantar a demo por meio desta automação você precisa de:

* Um Tenant do CDP em nuvem pública ou privada.
* Um Usuário de Workload CDP com políticas Ranger e Mappings de IDBroker configurados de acordo.
* Um Serviço CDE na versão 1.21 ou superior.
* O entitlement de uso do Runtime Docker Personalizado. Por favor, entre em contato com a equipe de produto ou vendas da CDE para obter o entitlement.
* Uma conta Dockerhub. Por favor, tenha pronto seu usuário e senha Dockerhub.

## Informações Importantes

A automação implanta o seguinte no seu Cluster Virtual CDE:

* Um Job Spark CDE e Recursos CDE associados com o objetivo de criar dados sintéticos no Armazenamento em Nuvem para cada participante.
* Um Recurso de Arquivos CDE para arquivos Spark compartilhados por todos os participantes chamado "Spark-Files-Shared".
* Um Recurso de Arquivos CDE para arquivos Airflow compartilhados por todos os participantes chamado "Airflow-Files-Shared".
* Um Recurso Python CDE compartilhado por todos os participantes chamado "Python-Env-Shared".

## Opções de Implantação

Há duas opções de implantação: Docker ou clonando este repositório git e executando os scripts de configuração localmente. A opção Docker é recomendada. Por favor, siga um dos dois conjuntos de instruções abaixo.

Quando a configuração estiver completa, navegue até a interface do CDE e valide se a execução do trabalho foi concluída com sucesso. Isso implica que os dados do HOL foram criados com sucesso no Armazenamento em Nuvem.

### 1. Instruções de Implantação com Docker

Um contêiner Docker foi criado com todas as dependências necessárias para a implantação do HOL.

```
% docker run -it pauldefusco/cde121holsetup
```

No shell do contêiner, modifique a URL da API Jobs na configuração CLI do CDE:

```
[cdeuser@1234567 ~]$ vi ~/.cde/config.yaml

user: <cdp_workload_username>
vcluster-endpoint: https://a1b2345.cde-abcdefg.cde-xxx.xxxxx.cloudera.site/dex/api/v1
```

Em seguida, execute o script de implantação com:

```
[cdeuser@1234567 ~]$ ./setup/deploy_hol.sh <docker-user> <cdp-workload-user> <number-of-participants> <storage-location>
```

Por exemplo:

```
#AWS
[cdeuser@1234567 ~]$ ./setup/deploy_hol.sh pauldefusco pauldefusco 3 s3a://goes-se-sandbox01/data
```

```
#Azure
[cdeuser@1234567 ~]$ ./setup/deploy_hol.sh pauldefusco pauldefusco 3 abfs://logs@go01demoazure.dfs.core.windows.net/data
```

### 2. Instruções de Implantação Local

Clone este repositório na sua máquina. Em seguida, execute o script de implantação com:

```
% ./setup/deploy_hol.sh <docker-user> <cdp-workload-user> <number-of-participants> <storage-location>
```

Por exemplo:

```
#AWS
% ./setup/deploy_hol.sh pauldefusco pauldefusco 3 s3a://goes-se-sandbox01/data
```

```
#Azure
% ./setup/deploy_hol.sh pauldefusco pauldefusco 3 abfs://logs@go01demoazure.dfs.core.windows.net/data
```

## Instruções de Desmontagem

Quando terminar, execute este script para desmontar os dados no Catálogo, mas não no S3. Esse passo será tratado pelos scripts de desmontagem do GOES.

```
% ./teardown.sh cdpworkloaduser
```

## Resumo

Você pode implantar uma demo completa do CDE com a automação fornecida. A demo executa um pequeno pipeline ETL que inclui Iceberg, Spark e Airflow.
