# Déploiement et Orchestration des Jobs

* [Laboratoire 3 : Créer des ressources CDE et des jobs Spark](https://github.com/pdefusco/CDE_121_HOL/blob/main/step_by_step_guides/english/part_02_deployment.md#lab-3-create-cde-resources-and-run-cde-spark-job)
* [Laboratoire 4 : Orchestrer un pipeline Spark avec Airflow](https://github.com/pdefusco/CDE_121_HOL/blob/main/step_by_step_guides/english/part_02_deployment.md#lab-4-orchestrate-spark-pipeline-with-airflow)
* [Introduction brève à Apache Airflow](https://github.com/pdefusco/CDE_121_HOL/blob/main/step_by_step_guides/english/part_02_deployment.md#a-brief-introduction-to-airflow)
* [Résumé](https://github.com/pdefusco/CDE_121_HOL/blob/main/step_by_step_guides/english/part_02_deployment.md#summary)
* [Liens et ressources utiles](https://github.com/pdefusco/CDE_121_HOL/blob/main/step_by_step_guides/english/part_02_deployment.md#useful-links-and-resources)


### Laboratoire 3 : Créer des ressources CDE et des jobs Spark

Jusqu'à présent, vous avez utilisé les sessions pour explorer les données de manière interactive. CDE vous permet également d'exécuter du code d'application Spark en mode batch en tant que Job CDE. Il existe deux types de Jobs CDE : Spark et Airflow. Dans ce laboratoire, nous allons créer un Job Airflow pour orchestrer trois Jobs Spark.

Le Job CDE est une abstraction sur le Spark Submit ou le DAG Airflow. Avec le Job Spark CDE, vous pouvez créer une définition modulaire et réutilisable qui est sauvegardée dans le cluster et peut être modifiée dans l'interface utilisateur CDE (ou via le CLI et l'API CDE) à chaque exécution. CDE stocke la définition du job pour chaque exécution dans l'interface utilisateur des Exécutions de Jobs, vous pouvez donc revenir et y faire référence longtemps après la fin de votre job.

De plus, CDE vous permet de stocker directement des artefacts tels que des fichiers Python, des Jars et d'autres dépendances, ou de créer des environnements Python et des conteneurs Docker dans CDE en tant que "Ressources CDE". Une fois créées dans CDE, les Ressources sont disponibles pour les Jobs CDE en tant que composants modulaires de la définition du Job CDE, pouvant être échangés et référencés par un job particulier si nécessaire.

Ces fonctionnalités réduisent considérablement l'effort autrement nécessaire pour gérer et surveiller les Jobs Spark et Airflow. En fournissant un panneau unifié sur toutes vos exécutions ainsi qu'une vue claire de tous les artefacts et dépendances associés, CDE rationalise les opérations Spark & Airflow.

##### Familiarisez-vous avec le Code

Les scripts d'application Spark et les fichiers de configuration utilisés dans ces laboratoires sont disponibles dans le [dossier des Jobs Spark CDE du dépôt git HOL](https://github.com/pdefusco/CDE_121_HOL/tree/main/cde_spark_jobs). Avant de passer à l'étape suivante, veuillez vous familiariser avec le code des fichiers "01_Lakehouse_Bronze.py", "002_Lakehouse_Silver.py", "003_Lakehouse_Gold.py", "utils.py", "parameters.conf".

Le script DAG Airflow est disponible dans le [dossier des Jobs Airflow CDE du dépôt git HOL](https://github.com/pdefusco/CDE_121_HOL/tree/main/cde_airflow_jobs). Veuillez également vous familiariser avec le code du script "airflow_dag.py".

* Le script "01_Lakehouse_Bronze.py" Application PySpark crée des tables Iceberg pour les transactions des clients et les cartes de crédit à partir de différents formats de fichiers. "utils.py" contient une méthode Python pour transformer plusieurs colonnes de dataframe à la fois, utilisée par le script "01_Lakehouse_Bronze.py".

* "parameters.conf" contient une variable de configuration qui est passée à chacun des trois scripts PySpark. Stocker des variables dans une Ressource Fichiers est une méthode couramment utilisée par les Ingénieurs de Données CDE pour paramétrer dynamiquement des scripts et passer des informations d'identification cachées à l'exécution.

* "02_Lakehouse_Silver.py" charge les données du nouveau fichier json de transactions, les valide avec Great Expectations et les ajoute à la table Transactions.

* "03_Lakehouse_Gold.py" charge les données de la table Transactions en filtrant par ID de snapshot Iceberg afin de ne refléter que le dernier lot. Ensuite, il les joint avec la table des clients et utilise une UDF PySpark pour filtrer les clients en fonction de la distance par rapport au lieu de la transaction. Enfin, il crée une table de la couche Gold pour fournir un accès curé aux analystes commerciaux et autres parties prenantes autorisées.

* "airflow_dag.py" orchestre le pipeline de Data Engineering. Tout d'abord, un bucket AWS S3 est créé ; un fichier simple "my_file.txt" est lu depuis une Ressource Fichiers CDE et écrit dans le bucket S3. Successivement, les trois Jobs Spark CDE discutés ci-dessus sont exécutés pour créer une table de la couche Gold du Lakehouse.

##### Créer un Dépôt CDE

Les dépôts Git permettent aux équipes de collaborer, de gérer les artefacts de projet et de promouvoir des applications de l'environnement de développement à l'environnement de production. CDE prend en charge l'intégration avec des fournisseurs de Git tels que GitHub, GitLab et Bitbucket pour synchroniser les exécutions de jobs avec différentes versions de votre code.

À cette étape, vous allez créer un Dépôt CDE pour cloner les scripts PySpark contenant le Code d'Application pour votre Job Spark CDE.

Depuis la Page Principale, cliquez sur "Dépôts" puis sur l'icône bleue "Créer un Dépôt".

![alt text](../../img/part3-repos-1.png)

Utilisez les paramètres suivants pour le formulaire :

```
Nom du Dépôt : CDE_Repo_userxxx
URL : https://github.com/pdefusco/CDE_121_HOL.git
Branche : main
```

![alt text](../../img/part3-repos-2.png)

Tous les fichiers du dépôt git sont maintenant stockés dans CDE en tant que Dépôt CDE. Chaque participant aura son propre dépôt CDE.

![alt text](../../img/part3-repos-3.png)

##### Créer une Ressource Fichiers CDE

Une ressource dans CDE est une collection nommée de fichiers utilisés par un job ou une session. Les ressources peuvent inclure du code d'application, des fichiers de configuration, des images Docker personnalisées et des spécifications d'environnement virtuel Python (requirements.txt). Les Ingénieurs de Données CDE exploitent les Ressources Fichiers pour stocker des fichiers et d'autres dépendances de jobs dans CDE, puis les associer aux Exécutions de Jobs.

Une Ressource CDE de type "Fichiers" contenant les fichiers "parameters.conf" et "utils.py" nommée "Spark_Files_Resource" a déjà été créée pour tous les participants.

##### Créer une Ressource Environnement Python CDE

Une Ressource CDE de type "Python" construite avec le fichier requirements.txt et nommée "Python_Resource" a déjà été créée pour tous les participants. Le requirements.txt inclut la liste des packages Python installés, qui seront utilisés par l'Exécution de Job lorsqu'elle y sera attachée.

Pour ce laboratoire, nous avons inclus Great Expectations, un framework populaire pour la qualité et la validation des données.

##### Créer un Job Spark CDE

Maintenant que vous êtes familiarisé avec les Dépôts et les Ressources CDE, vous êtes prêt à créer votre premier Job Spark CDE.

Naviguez jusqu'à l'onglet des Jobs CDE et cliquez sur "Créer un Job". Le long formulaire chargé sur la page vous permet de construire un Spark Submit en tant que Job Spark CDE, étape par étape.

Entrez les valeurs suivantes sans les guillemets dans les champs correspondants. Assurez-vous de mettre à jour le nom d'utilisateur avec votre utilisateur attribué partout où cela est nécessaire :

```
* Type de Job : Spark
* Nom : 001_Lakehouse_Bronze_userxxx
* Fichier : Sélectionner dans le Dépôt -> "cde_spark_jobs/001_Lakehouse_Bronze.py"
* Arguments : userxxx #e.g. user002
* Options Avancées - Ressources : Spark_Files_Resource
* Options Avancées - Dépôts : CDE_Repo_userxxx e.g. CDE_Repo_user002
* Options de Calcul - augmenter "Cores du Executor" et "Mémoire du Executor" de 1 à 2.
```

Enfin, enregistrez le Job CDE en cliquant sur l'icône "Créer". ***Veuillez ne pas sélectionner "Créer et Exécuter".***

![alt text](../../img/new_spark_job_1.png)

![alt text](../../img/new_spark_job_2.png)

Répétez le processus pour les scripts PySpark restants :

Job Lakehouse Silver :

```
* Type de Job :

 Spark
* Nom : 002_Lakehouse_Silver_userxxx
* Fichier : Sélectionner dans le Dépôt -> "002_Lakehouse_Silver.py"
* Arguments : userxxx #e.g. user002
* Environnement Python : Python_Resource
* Options Avancées - Ressources : Spark_Files_Resource
* Options Avancées - Dépôts : CDE_Repo_userxxx e.g. CDE_Repo_user002
```

Job Lakehouse Gold :

```
* Type de Job : Spark
* Nom : 003_Lakehouse_Gold_userxxx
* Fichier : Sélectionner dans le Dépôt -> "003_Lakehouse_Gold.py"
* Arguments : userxxx #e.g. user002
* Options Avancées - Ressources : Spark_Files_Resource
* Options Avancées - Dépôts : CDE_Repo_userxxx e.g. CDE_Repo_user002
```

Encore une fois, ***veuillez créer mais ne pas exécuter les jobs !***

![alt text](../../img/spark_job_create.png)


### Laboratoire 4 : Orchestrer un pipeline Spark avec Airflow

Dans ce laboratoire, vous allez construire un pipeline de Jobs Spark pour charger un nouveau lot de transactions, les joindre avec les données PII des clients, et créer une table des clients susceptibles d'être victimes de fraude par carte de crédit, y compris leur adresse e-mail et leur nom. L'ensemble du flux de donnés sera orchestré par Apache Airflow.

### Introduction brève à Airflow

Apache Airflow est une plateforme pour concevoir, planifier et exécuter des pipelines de Data Engineering. Il est largement utilisé par la communauté pour créer des workflows dynamiques et robustes pour des cas d'utilisation de Data Engineering en batch.

La caractéristique principale des workflows Airflow est que tous les workflows sont définis en code Python. Le code Python définissant le workflow est stocké sous forme de collection de Tâches Airflow organisées dans un DAG. Les tâches sont définies par des opérateurs intégrés et des modules Airflow. Les opérateurs sont des classes Python qui peuvent être instanciées pour effectuer des actions prédéfinies et paramétrées.

CDE intègre Apache Airflow au niveau du Cluster Virtuel CDE. Il est automatiquement déployé pour l'utilisateur CDE lors de la création du Cluster Virtuel CDE et ne nécessite aucune maintenance de la part de l'Administrateur CDE. En plus des opérateurs de base, CDE prend en charge le CDEJobRunOperator et le CDWOperator pour déclencher des Jobs Spark et des requêtes Datawarehousing.

##### Créer une Ressource Fichiers Airflow

Comme pour les Jobs Spark CDE, les jobs Airflow peuvent tirer parti des Ressources Fichiers CDE pour charger des fichiers, y compris des ensembles de données ou des paramètres d'exécution. Une Ressource Fichiers CDE nommée "Airflow_Files_Resource" contenant le fichier "my_file.txt" a déjà été créée pour tous les participants.

##### Créer un Job Airflow

Ouvrez le script "004_airflow_dag.py" situé dans le dossier "cde_airflow_jobs". Familiarisez-vous avec le code et notez :

* Les classes Python nécessaires aux DAG Operators sont importées en haut. Notez que le CDEJobRunOperator est inclus pour exécuter des Jobs Spark dans CDE.
* Le dictionnaire "default_args" inclut des options pour la planification, la définition des dépendances et l'exécution générale.
* Trois instances de l'objet CDEJobRunOperator sont déclarées. Celles-ci reflètent les trois Jobs Spark CDE que vous avez créés ci-dessus.
* Enfin, au bas du DAG, les Dépendances des Tâches sont déclarées. Avec cette déclaration, vous pouvez spécifier la séquence d'exécution des tâches du DAG.

Téléchargez le fichier depuis [ce URL](https://github.com/pdefusco/CDE_121_HOL/tree/main/cde_airflow_jobs) sur votre machine locale. Ouvrez-le dans votre éditeur de choix et modifiez la variable d'utilisateur à la ligne 52.

Ensuite, naviguez jusqu'à l'interface utilisateur des Jobs CDE et créez un nouveau Job CDE. Sélectionnez Airflow comme Type de Job. Sélectionnez le script "004_airflow_dag.py" et choisissez de créer une nouvelle Ressource Fichiers nommée d'après vous dans le processus. Enfin, ajoutez la dépendance de la Ressource Fichiers où vous avez chargé "my_file.txt".

![alt text](../../img/new_airflow_job_1.png)

![alt text](../../img/new_airflow_job_2.png)

Surveillez l'exécution du pipeline depuis l'interface des Exécutions de Jobs. Notez qu'un Job Airflow sera déclenché et successivement les trois Jobs Spark CDE s'exécuteront un par un.

Pendant que le job est en cours, ouvrez l'interface Airflow et surveillez l'exécution.

![alt text](../../img/new_airflow_job_3.png)


### Résumé

Cloudera Data Engineering (CDE) est un service sans serveur pour la Cloudera Data Platform qui vous permet de soumettre des jobs batch à des clusters virtuels auto-scaling. CDE vous permet de passer plus de temps sur vos applications et moins de temps sur l'infrastructure.

Dans ces laboratoires, vous avez amélioré votre code pour le rendre réutilisable en modulant votre logique en fonctions, et stocké ces fonctions en tant qu'utilitaire dans une Ressource Fichiers CDE. Vous avez exploité votre Ressource Fichiers en stockant des variables dynamiques dans un fichier de configuration de paramètres et en appliquant une variable à l'exécution via le champ Arguments. Dans le cadre de pipelines Spark CI/CD plus avancés, le fichier de paramètres et le champ Arguments peuvent être écrasés et remplacés à l'exécution.

Vous avez ensuite utilisé Apache Airflow non seulement pour orchestrer ces trois jobs, mais aussi pour les exécuter dans le cadre d'un pipeline de Data Engineering plus complexe qui touchait des ressources dans AWS. Grâce au vaste écosystème de fournisseurs open source d'Airflow, vous pouvez également opérer sur des systèmes externes et tiers.

Enfin, vous avez exécuté le job et observé les sorties sur la page des Exécutions de Jobs CDE. CDE a stocké les Exécutions de Jobs, les journaux et les Ressources CDE associées pour chaque exécution. Cela vous a fourni des capacités de surveillance et de dépannage en temps réel, ainsi qu'un stockage post-exécution des journaux, des dépendances d'exécution et des informations sur le cluster. Vous explorerez la Surveillance et l'Observabilité plus en détail dans les prochains laboratoires.


### Liens et ressources utiles

* [Travailler avec les Ressources Fichiers CDE](https://community.cloudera.com/t5/Community-Articles/Working-with-CDE-Files-Resources/ta-p/379891)
* [Surveiller efficacement les Jobs, les Exécutions et les Ressources avec le CLI CDE](https://community.cloudera.com/t5/Community-Articles/Efficiently-Monitoring-Jobs-Runs-and-Resources-with-the-CDE/ta-p/379893)
* [Travailler avec les Paramètres de Job Spark CDE dans Cloudera Data Engineering](https://community.cloudera.com/t5/Community-Articles/Working-with-CDE-Spark-Job-Parameters-in-Cloudera-Data/ta-p/380792)
* [Comment analyser les fichiers XML dans CDE avec le Package XML Spark](https://community.cloudera.com/t5/Community-Articles/How-to-parse-XMLs-in-Cloudera-Data-Engineering-with-the/ta-p/379451)
* [Spark Géospatial avec Apache Sedona dans CDE](https://community.cloudera.com/t5/Community-Articles/Spark-Geospatial-with-Apache-Sedona-in-Cloudera-Data/ta-p/378086)
* [Automatiser les Pipelines de Données en utilisant Apache Airflow dans CDE](https://docs.cloudera.com/data-engineering/cloud/orchestrate-workflows/topics/cde-airflow-dag-pipeline.html)
* [Utiliser CDE Airflow](https://github.com/pdefusco/Using_CDE_Airflow)
* [Documentation sur les Arguments des DAG Airflow](https://airflow.apache.org/docs/apache-airflow/stable/tutorial.html#default-arguments)
* [Explorer l'Architecture Iceberg](https://github.com/pdefusco/Exploring_Iceberg_Architecture)
* [Qualité des Données d'Entreprise à Grande Échelle dans CDE avec Great Expectations et les Runtimes Personnalisés CDE](https://community.cloudera.com/t5/Community-Articles/Enterprise-Data-Quality-at-Scale-with-Spark-and-Great/ta-p/378161)
