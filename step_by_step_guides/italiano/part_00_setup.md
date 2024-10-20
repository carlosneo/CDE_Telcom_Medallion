# Setup Automatico

## Obiettivo

Questo repository git ospita l'automazione della configurazione per il HOL. Lo script consiste in una serie di comandi CDE CLI che prima creano dati sintetici nello storage cloud per ogni partecipante e poi creano la Risorsa CDE condivisa da ogni partecipante.

Questa automazione non esegue il materiale del laboratorio effettivo. Questi sono facili da creare tramite l'interfaccia utente CDE e non richiedono automazione.

## Indice

* [Requisiti](https://github.com/pdefusco/CDE_121_HOL/blob/main/step_by_step_guides/italian/part_00_setup.md#requisiti)
* [Informazioni Importanti](https://github.com/pdefusco/CDE_121_HOL/blob/main/step_by_step_guides/italian/part_00_setup.md#informazioni-importanti)
* [Opzioni di Implementazione](https://github.com/pdefusco/CDE_121_HOL/blob/main/step_by_step_guides/italian/part_00_setup.md#opzioni-di-implementazione)
  * [1. Istruzioni di Implementazione con Docker](https://github.com/pdefusco/CDE_121_HOL/blob/main/step_by_step_guides/italian/part_00_setup.md#1-istruzioni-di-implementazione-con-docker)
  * [2. Istruzioni di Implementazione Locale](https://github.com/pdefusco/CDE_121_HOL/blob/main/step_by_step_guides/italian/part_00_setup.md#2-istruzioni-di-implementazione-locale)
* [Istruzioni di Smantellamento](https://github.com/pdefusco/CDE_121_HOL/blob/main/step_by_step_guides/italian/part_00_setup.md#istruzioni-di-smantellamento)
* [Riepilogo](https://github.com/pdefusco/CDE_121_HOL/blob/main/step_by_step_guides/italian/part_00_setup.md#riepilogo)

## Requisiti

Per distribuire la demo tramite questa automazione è necessario:

* Un tenant CDP nel cloud pubblico o privato.
* Un Utente di Workload CDP con politiche Ranger e Mappings IDBroker configurati di conseguenza.
* Un Servizio CDE in versione 1.21 o superiore.
* Il diritto di utilizzo del Runtime Docker Personalizzato. Si prega di contattare il team prodotto o vendite di CDE per ottenere il diritto.
* Un account Dockerhub. Si prega di avere a disposizione l'utente e la password di Dockerhub.

## Informazioni Importanti

L'automazione distribuisce quanto segue nel tuo Cluster Virtuale CDE:

* Un Job Spark CDE e Risorse CDE associate con lo scopo di creare dati sintetici nello Storage Cloud per ogni partecipante.
* Una Risorsa di File CDE per i file Spark condivisi da tutti i partecipanti chiamata "Spark-Files-Shared".
* Una Risorsa di File CDE per i file Airflow condivisi da tutti i partecipanti chiamata "Airflow-Files-Shared".
* Una Risorsa Python CDE condivisa da tutti i partecipanti chiamata "Python-Env-Shared".

## Opzioni di Implementazione

Ci sono due opzioni di implementazione: Docker o clonare questo repository git ed eseguire gli script di configurazione localmente. L'opzione Docker è consigliata. Si prega di seguire uno dei due set di istruzioni di seguito.

Quando la configurazione è completa, accedi all'interfaccia utente di CDE e verifica che l'esecuzione del job sia completata con successo. Questo implica che i dati HOL sono stati creati con successo nello Storage Cloud.

### 1. Istruzioni di Implementazione con Docker

È stato creato un contenitore Docker con tutte le dipendenze necessarie per l'implementazione del HOL.

```
% docker run -it pauldefusco/cde121holsetup
```

Nel shell del contenitore, modifica l'URL dell'API Jobs nella configurazione CLI di CDE:

```
[cdeuser@1234567 ~]$ vi ~/.cde/config.yaml

user: <cdp_workload_username>
vcluster-endpoint: https://a1b2345.cde-abcdefg.cde-xxx.xxxxx.cloudera.site/dex/api/v1
```

Poi esegui lo script di distribuzione con:

```
[cdeuser@1234567 ~]$ ./setup/deploy_hol.sh <docker-user> <cdp-workload-user> <number-of-participants> <storage-location>
```

Ad esempio:

```
#AWS
[cdeuser@1234567 ~]$ ./setup/deploy_hol.sh pauldefusco pauldefusco 3 s3a://goes-se-sandbox01/data
```

```
#Azure
[cdeuser@1234567 ~]$ ./setup/deploy_hol.sh pauldefusco pauldefusco 3 abfs://logs@go01demoazure.dfs.core.windows.net/data
```

### 2. Istruzioni di Implementazione Locale

Clona questo repository sulla tua macchina. Poi esegui lo script di distribuzione con:

```
% ./setup/deploy_hol.sh <docker-user> <cdp-workload-user> <number-of-participants> <storage-location>
```

Ad esempio:

```
#AWS
% ./setup/deploy_hol.sh pauldefusco pauldefusco 3 s3a://goes-se-sandbox01/data
```

```
#Azure
% ./setup/deploy_hol.sh pauldefusco pauldefusco 3 abfs://logs@go01demoazure.dfs.core.windows.net/data
```

## Istruzioni di Smantellamento

Quando hai finito, esegui questo script per smantellare i dati nel Catalogo ma non in S3. Questo passaggio sarà gestito dagli script di smantellamento di GOES.

```
% ./teardown.sh cdpworkloaduser
```

## Riepilogo

Puoi distribuire una demo completa di CDE con l'automazione fornita. La demo esegue una piccola pipeline ETL che include Iceberg, Spark e Airflow.
