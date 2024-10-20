# Setup Automatique

## Objectif

Ce dépôt Git héberge l'automatisation de la configuration pour le HOL. Le script consiste en une série de commandes CDE CLI qui créent d'abord des données synthétiques dans le stockage cloud pour chaque participant, puis créent le Ressource CDE partagé par chaque participant.

Cette automatisation n'exécute pas le matériel de laboratoire réel. Ceux-ci sont faciles à créer depuis l'interface utilisateur CDE et ne nécessitent pas d'automatisation.

## Table des Matières

* [Exigences](https://github.com/pdefusco/CDE_121_HOL/blob/main/step_by_step_guides/french/part_00_setup.md#exigences)
* [Informations Importantes](https://github.com/pdefusco/CDE_121_HOL/blob/main/step_by_step_guides/french/part_00_setup.md#informations-importantes)
* [Options de Déploiement](https://github.com/pdefusco/CDE_121_HOL/blob/main/step_by_step_guides/french/part_00_setup.md#options-de-déploiement)
  * [1. Instructions de Déploiement avec Docker](https://github.com/pdefusco/CDE_121_HOL/blob/main/step_by_step_guides/french/part_00_setup.md#1-instructions-de-déploiement-avec-docker)
  * [2. Instructions de Déploiement Local](https://github.com/pdefusco/CDE_121_HOL/blob/main/step_by_step_guides/french/part_00_setup.md#2-instructions-de-déploiement-local)
* [Instructions de Désinstallation](https://github.com/pdefusco/CDE_121_HOL/blob/main/step_by_step_guides/french/part_00_setup.md#instructions-de-désinstallation)
* [Résumé](https://github.com/pdefusco/CDE_121_HOL/blob/main/step_by_step_guides/french/part_00_setup.md#résumé)

## Exigences

Pour déployer la démo via cette automatisation, vous avez besoin de :

* Un locataire CDP dans le cloud public ou privé.
* Un CDP Workload User avec des politiques Ranger et des Mappings IDBroker configurés en conséquence.
* Un Service CDE en version 1.21 ou supérieure.
* L'autorisation de Runtime Docker Personnalisé. Veuillez contacter l'équipe produit ou commerciale de CDE pour obtenir l'autorisation.
* Un compte Dockerhub. Veuillez avoir votre utilisateur et mot de passe Dockerhub prêts.

## Informations Importantes

L'automatisation déploie ce qui suit dans votre Cluster Virtuel CDE :

* Un Job Spark CDE et les Ressources CDE associées dans le but de créer des données synthétiques dans le Stockage Cloud pour chaque participant.
* Une Ressource de Fichiers CDE pour les fichiers Spark partagés par tous les participants, nommée "Spark-Files-Shared".
* Une Ressource de Fichiers CDE pour les fichiers Airflow partagés par tous les participants, nommée "Airflow-Files-Shared".
* Une Ressource Python CDE partagée par tous les participants, nommée "Python-Env-Shared".

## Options

 de Déploiement

Il existe deux options de déploiement : Docker ou clonage de ce dépôt git et exécution des scripts de configuration localement. L'option Docker est recommandée. Veuillez suivre l'un des deux ensembles d'instructions ci-dessous.

Une fois la configuration terminée, accédez à l'interface utilisateur CDE et vérifiez que l'exécution du job a été complétée avec succès. Cela implique que les données HOL ont été créées avec succès dans le Stockage Cloud.

### 1. Instructions de Déploiement avec Docker

Un conteneur Docker a été créé avec toutes les dépendances nécessaires pour le déploiement du HOL.

```
% docker run -it pauldefusco/cde121holsetup
```

Dans le shell du conteneur, modifiez l'URL de l'API Jobs dans la configuration CLI de CDE :

```
[cdeuser@1234567 ~]$ vi ~/.cde/config.yaml

user: <cdp_workload_username>
vcluster-endpoint: https://a1b2345.cde-abcdefg.cde-xxx.xxxxx.cloudera.site/dex/api/v1
```

Puis exécutez le script de déploiement avec :

```
[cdeuser@1234567 ~]$ ./setup/deploy_hol.sh <docker-user> <cdp-workload-user> <number-of-participants> <storage-location>
```

Par exemple :

```
#AWS
[cdeuser@1234567 ~]$ ./setup/deploy_hol.sh pauldefusco pauldefusco 3 s3a://goes-se-sandbox01/data
```

```
#Azure
[cdeuser@1234567 ~]$ ./setup/deploy_hol.sh pauldefusco pauldefusco 3 abfs://logs@go01demoazure.dfs.core.windows.net/data
```

### 2. Instructions de Déploiement Local

Clonez ce dépôt sur votre machine. Ensuite, exécutez le script de déploiement avec :

```
% ./setup/deploy_hol.sh <docker-user> <cdp-workload-user> <number-of-participants> <storage-location>
```

Par exemple :

```
#AWS
% ./setup/deploy_hol.sh pauldefusco pauldefusco 3 s3a://goes-se-sandbox01/data
```

```
#Azure
% ./setup/deploy_hol.sh pauldefusco pauldefusco 3 abfs://logs@go01demoazure.dfs.core.windows.net/data
```

## Instructions de Désinstallation

Lorsque vous avez terminé, exécutez ce script pour démonter les données dans le Catalogue mais pas dans S3. Cette étape sera gérée par les scripts de démontage GOES.

```
% ./teardown.sh cdpworkloaduser
```

## Résumé

Vous pouvez déployer une démo complète de CDE avec l'automatisation fournie. La démo exécute un petit pipeline ETL incluant Iceberg, Spark et Airflow.
