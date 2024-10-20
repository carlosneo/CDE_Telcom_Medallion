# Observabilité des Jobs et Gouvernance des Données

* [Lab 5 : Surveillance des Jobs avec Cloudera Observability et CDE](https://github.com/pdefusco/CDE_121_HOL/blob/main/step_by_step_guides/french/part_03_observability.md#lab-5-surveillance-des-jobs-avec-cloudera-observability-et-cde)
* [Lab 6 : Gouvernance des Jobs Spark avec le CDP Data Catalog](https://github.com/pdefusco/CDE_121_HOL/blob/main/step_by_step_guides/french/part_03_observability.md#lab-6-gouvernance-des-jobs-spark-avec-le-cdp-data-catalog)
* [Résumé](https://github.com/pdefusco/CDE_121_HOL/blob/main/step_by_step_guides/french/part_03_observability.md#résumé)
* [Liens et Ressources Utiles](https://github.com/pdefusco/CDE_121_HOL/blob/main/step_by_step_guides/french/part_03_observability.md#liens-et-ressources-utiles)


### Lab 5 : Surveillance des Jobs avec Cloudera Observability et CDE

CDE propose une fonctionnalité d'observabilité des jobs intégrée, incluant une interface Job Runs, l'interface Airflow, et la possibilité de télécharger les métadonnées et les journaux des jobs via l'API et la CLI de CDE.

Cloudera Observability est un service Cloudera qui vous aide à comprendre de manière interactive votre environnement, vos services de données, vos charges de , vos clusters et vos ressources à travers tous les services de calcul dans un environnement CDP - ***y compris CDE***.

Lorsque une charge de job se termine, les informations de diagnostic sur le job ou la requête et le cluster qui les a traités sont collectées par Telemetry Publisher et envoyées à Cloudera Observability, afin que vous puissiez optimiser vos requêtes et pipelines grâce à :

* Une large gamme de métriques et de tests de santé qui vous aident à identifier et résoudre à la fois les problèmes existants et potentiels.
* Des conseils et des recommandations prescriptives qui vous aident à résoudre rapidement ces problèmes et à optimiser les solutions.
* Des références de performance et des analyses historiques qui vous aident à identifier et résoudre les problèmes de performance.

De plus, Cloudera Observability vous permet également de :

* Afficher visuellement les coûts actuels et historiques de votre cluster, ce qui vous aide à planifier et prévoir les budgets, les environnements de charges de job futurs, et à justifier les groupes et ressources utilisateur actuels.
* Déclencher des actions en temps réel à travers les jobs et les requêtes, ce qui vous aide à prendre des mesures pour atténuer les problèmes potentiels.
* Recevoir quotidiennement les statistiques de votre cas d'utilisation par e-mail, ce qui vous aide à suivre, comparer et surveiller sans avoir à vous connecter au cluster.
* Décomposer les métriques de votre cluster en vues plus significatives pour vos besoins commerciaux, ce qui vous aide à analyser des critères spécifiques de cas d'utilisation. Par exemple, vous pouvez analyser comment les requêtes qui accèdent à une base de données particulière ou qui utilisent un pool de ressources spécifique performent par rapport à vos SLA. Ou vous pouvez examiner comment toutes les requêtes envoyées par un utilisateur spécifique performent sur votre cluster.

##### Surveiller les Jobs dans CDE

Accédez au cluster virtuel CDE ObservabilityLabs. Ouvrez l'interface Job Runs et remarquez que ce cluster a déjà été configuré avec un pipeline Airflow composé de trois jobs Spark qui chargent progressivement un nouveau lot de données toutes les cinq minutes.

Sélectionnez le dernier Job Run du job nommé "airflow-orchestration-pauldefusco-mfct", ouvrez l'onglet "Logs" et explorez chaque sortie des tâches Airflow. Chaque tâche correspond à un CDEJobRunOperator dans le code DAG d'Airflow.

![alt text](../../img/new_airflow_run_1.png)

Ensuite, revenez à la page des Job Runs et explorez le dernier Job Run pour le job "iceberg_mergeinto-pauldefusco-mfct". Explorez l'onglet Configuration, Logs et Spark UI.

* L'onglet Configuration stocke les dépendances et configurations des jobs Spark.
* L'onglet Logs donne accès aux journaux de l'application Spark.
* La Spark UI offre une visibilité sur l'exécution du job Spark.

Tout cela est conservé dans l'interface Job Runs de CDE pour que les ingénieurs de données CDE puissent facilement valider les exécutions passées et effectuer un dépannage complexe.

![alt text](../../img/new_airflow_run_2.png)

##### Surveiller les Jobs dans CDP Observability

Retournez de CDE à la page d'accueil CDP, puis ouvrez CDP Observability. Développez le cluster ObservabilityLabs puis l'onglet "Spark".

![alt text](../../img/new_obs_1.png)

![alt text](../../img/new_obs_2.png)

![alt text](../../img/new_obs_3.png)

Explorez les tendances agrégées des jobs. Notez que les jobs prennent de plus en plus de temps à s'exécuter. Cela est dû au fait que les données sont chargées progressivement dans la table Iceberg avec une opération Merge Into qui opère sur une table de plus en plus grande.

Sélectionnez le job avec la durée la plus longue et explorez l'onglet Execution Details pour trouver des informations au niveau du job Spark et du stage, et l'onglet Baseline pour trouver des métriques d'exécution Spark détaillées. Dans l'onglet Baseline, cliquez sur l'icône "Show Abnormal Metrics" pour identifier les problèmes potentiels avec votre exécution particulière du job.

![alt text](../../img/new_obs_4.png)

![alt text](../../img/new_obs_5.png)


### Lab 6 : Gouvernance des Jobs Spark avec le CDP Data Catalog

Le CDP Data Catalog est un service au sein de CDP qui vous permet de comprendre, gérer, sécuriser et gouverner les actifs de données à travers l'entreprise. Data Catalog vous aide à comprendre les données à travers plusieurs clusters et environnements CDP. Avec Data Catalog, vous pouvez comprendre comment les données sont interprétées pour l'utilisation, comment elles sont créées et modifiées, et comment l'accès aux données est sécurisé et protégé.

##### Explorer les Jobs dans Apache Atlas

Retournez à la page d'accueil CDP, ouvrez Data Catalog puis Atlas.

![alt text](../../img/catalog_1.png)

![alt text](../../img/catalog_2.png)

Atlas représente les métadonnées sous forme de types et d'entités, et fournit des capacités de gestion et de gouvernance des métadonnées pour les organisations afin de construire, catégoriser et gouverner les actifs de données.

Recherchez "spark_applications" dans la barre de recherche, puis sélectionnez une application Spark de la liste et explorez ses métadonnées.

![alt text](../../img/catalog_3.png)

![alt text](../../img/catalog_4.png)

Dans le panneau Classifications, créez une nouvelle Classification de Métadonnées. Assurez-vous d'utiliser un nom unique.

![alt text](../../img/catalog_5.png)

Retournez à la page principale, trouvez une application Spark et ouvrez-la. Appliquez ensuite la Classification de Métadonnées nouvellement créée.

![alt text](../../img/catalog_6.png)

![alt text](../../img/catalog_7.png)

Enfin, effectuez une nouvelle recherche, cette fois en utilisant la Classification que vous avez créée pour filtrer les Applications Spark.

![alt text](../../img/catalog_8.png)


### Résumé

Cloudera Observability est la solution d'observabilité de CDP, fournissant une vue unique pour découvrir et collecter en continu des télémétries de performance à travers les données, les applications et les composants d'infrastructure exécutés dans les déploiements CDP sur des clouds privés et publics. Avec des analyses et des corrélations avancées et intelligentes, elle fournit des insights et des recommandations pour résoudre les problèmes complexes, optimiser les coûts et améliorer les performances.

CDP Data Catalog est un service de gestion des métadonnées dans le cloud qui aide les organisations à trouver, gérer et comprendre leurs données dans le cloud. C'est un référentiel centralisé qui peut aider à la prise de décision basée sur les données, améliorer la gestion des données et accroître l'efficacité opérationnelle.

Dans cette section finale des labs, vous avez exploré les capacités de surveillance des Job Runs dans CDE. En particulier, vous avez utilisé l'interface Job Runs de CDE pour conserver les métadonnées des Job Runs, les journaux Spark et la Spark UI après l'exécution. Ensuite, vous avez utilisé CDP Observability pour explorer les métriques granulaires des Job Runs et détecter les anomalies. Enfin, vous avez utilisé CDP Data Catalog pour classifier les exécutions des jobs Spark afin de gouverner et rechercher des métadonnées importantes des Job Runs.

### Liens et Ressources Utiles

* [Documentation Cloudera Observability](https://docs.cloudera.com/observability/cloud/index.html)
* [CDP Data Catalog](https://docs.cloudera.com/data-catalog/cloud/index.html)
* [Documentation Apache Atlas](https://docs.cloudera.com/cdp-reference-architectures/latest/cdp-ra-security/topics/cdp-ra-security-apache-atlas.html)
* [Documentation Apache Ranger](https://docs.cloudera.com/cdp-reference-architectures/latest/cdp-ra-security/topics/cdp-ra-security-apache-ranger.html)
* [Surveillance Efficace des Jobs, des Exécutions et des Ressources avec la CLI CDE](https://community.cloudera.com/t5/Community-Articles/Surveillance-Efficace-des-Jobs-Des-Exécutions-et-Des-Ressources-avec-la-CLI-CDE/ta-p/379893)

---
