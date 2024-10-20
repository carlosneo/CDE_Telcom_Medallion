# Distribuzione e Orchestrazione dei Job

* [Laboratorio 3: Creare risorse CDE e job Spark](https://github.com/pdefusco/CDE_121_HOL/blob/main/step_by_step_guides/italian/part_02_deployment.md#lab-3-creare-risorse-cde-e-job-spark)
* [Laboratorio 4: Orchestrare un pipeline Spark con Airflow](https://github.com/pdefusco/CDE_121_HOL/blob/main/step_by_step_guides/italian/part_02_deployment.md#lab-4-orchestrare-pipeline-spark-con-airflow)
* [Introduzione breve ad Apache Airflow](https://github.com/pdefusco/CDE_121_HOL/blob/main/step_by_step_guides/italian/part_02_deployment.md#a-brief-introduction-to-airflow)
* [Riepilogo](https://github.com/pdefusco/CDE_121_HOL/blob/main/step_by_step_guides/italian/part_02_deployment.md#summary)
* [Link e risorse utili](https://github.com/pdefusco/CDE_121_HOL/blob/main/step_by_step_guides/italian/part_02_deployment.md#useful-links-and-resources)


### Laboratorio 3: Creare risorse CDE e job Spark

Fino ad ora, hai utilizzato sessioni per esplorare i dati in modo interattivo. CDE ti consente anche di eseguire il codice dell'applicazione Spark in modalità batch come Job CDE. Esistono due tipi di Job CDE: Spark e Airflow. In questo laboratorio, creerai un Job Airflow per orchestrare tre Job Spark.

Il Job CDE è un'astrazione su Spark Submit o DAG Airflow. Con il Job Spark CDE, puoi creare una definizione modulare e riutilizzabile che viene salvata nel cluster e può essere modificata tramite l'interfaccia utente CDE (o tramite CLI e API CDE) ad ogni esecuzione. CDE memorizza la definizione del job per ogni esecuzione nella pagina di Esecuzioni dei Jobs, quindi puoi tornare e fare riferimento ad essa molto tempo dopo il completamento del tuo job.

Inoltre, CDE ti consente di memorizzare direttamente artefatti come file Python, Jars e altre dipendenze, o creare ambienti Python e contenitori Docker in CDE come "Risorse CDE". Una volta creati in CDE, le Risorse sono disponibili per i Job CDE come componenti modulari della definizione del Job CDE, che possono essere scambiati e referenziati da un job particolare se necessario.

Queste funzionalità riducono notevolmente lo sforzo necessario per gestire e monitorare i Job Spark e Airflow. Fornendo un pannello unificato per tutte le tue esecuzioni e una vista chiara di tutti gli artefatti e le dipendenze associate, CDE semplifica le operazioni di Spark & Airflow.

##### Familiarizzati con il Codice

Gli script dell'applicazione Spark e i file di configurazione utilizzati in questi laboratori sono disponibili nella [cartella dei Job Spark CDE del repository git HOL](https://github.com/pdefusco/CDE_121_HOL/tree/main/cde_spark_jobs). Prima di passare al passaggio successivo, familiarizzati con il codice dei file "01_Lakehouse_Bronze.py", "002_Lakehouse_Silver.py", "003_Lakehouse_Gold.py", "utils.py", "parameters.conf".

Lo script DAG Airflow è disponibile nella [cartella dei Job Airflow CDE del repository git HOL](https://github.com/pdefusco/CDE_121_HOL/tree/main/cde_airflow_jobs). Familiarizzati anche con il codice dello script "airflow_dag.py".

* Lo script "01_Lakehouse_Bronze.py" dell'applicazione PySpark crea tabelle Iceberg per le transazioni dei clienti e le carte di credito a partire da diversi formati di file. "utils.py" contiene un metodo Python per trasformare più colonne di dataframe contemporaneamente, utilizzato dallo script "01_Lakehouse_Bronze.py".

* "parameters.conf" contiene una variabile di configurazione passata a ciascuno dei tre script PySpark. Memorizzare variabili in una Risorsa di File è un metodo comunemente utilizzato dagli Ingegneri dei Dati CDE per parametizzare dinamicamente script e passare credenziali nascoste durante l'esecuzione.

* "02_Lakehouse_Silver.py" carica i dati dal nuovo file json delle transazioni, li valida con Great Expectations e li aggiunge alla tabella Transactions.

* "03_Lakehouse_Gold.py" carica i dati dalla tabella Transactions filtrando per ID di snapshot Iceberg per riflettere solo l'ultimo batch. Successivamente, li unisce con la tabella clienti e utilizza una UDF PySpark per filtrare i clienti in base alla distanza dal luogo della transazione. Infine, crea una tabella della layer Gold per fornire accesso curato agli analisti aziendali e ad altre parti interessate autorizzate.

* "airflow_dag.py" orchestra il pipeline di Data Engineering. Prima, viene creato un bucket AWS S3; viene letto un file semplice "my_file.txt" da una Risorsa di File CDE e scritto nel bucket S3. Successivamente, vengono eseguiti i tre Job Spark CDE discussi in precedenza per creare una tabella della layer Gold del Lakehouse.

##### Creare un Repository CDE

I repository Git consentono ai team di collaborare, gestire artefatti del progetto e promuovere applicazioni dall'ambiente di sviluppo all'ambiente di produzione. CDE supporta l'integrazione con fornitori di Git come GitHub, GitLab e Bitbucket per sincronizzare le esecuzioni dei job con diverse versioni del tuo codice.

A questo punto, creerai un Repository CDE per clonare gli script PySpark che contengono il Codice dell'Applicazione per il tuo Job Spark CDE.

Dalla Pagina Principale, fai clic su "Repository" e poi sull'icona blu "Crea Repository".

![alt text](../../img/part3-repos-1.png)

Utilizza i seguenti parametri per il modulo:

```
Nome del Repository: CDE_Repo_userxxx
URL: https://github.com/pdefusco/CDE_121_HOL.git
Ramo: main
```

![alt text](../../img/part3-repos-2.png)

Tutti i file del repository git sono ora memorizzati in CDE come un Repository CDE. Ogni partecipante avrà il proprio repository CDE.

![alt text](../../img/part3-repos-3.png)

##### Creare una Risorsa di File CDE

Una risorsa in CDE è una raccolta nominata di file utilizzati da un job o una sessione. Le risorse possono includere codice dell'applicazione, file di configurazione, immagini Docker personalizzate e specifiche dell'ambiente virtuale Python (requirements.txt). Gli Ingegneri dei Dati CDE sfruttano le Risorse di File per memorizzare file e altre dipendenze di job in CDE, e poi associarli alle Esecuzioni dei Job.

Una Risorsa CDE di tipo "File" che contiene i file "parameters.conf" e "utils.py" denominata "Spark_Files_Resource" è già stata creata per tutti i partecipanti.

##### Creare una Risorsa di Ambiente Python CDE

Una Risorsa CDE di tipo "Python" costruita con il file requirements.txt e denominata "Python_Resource" è già stata creata per tutti i partecipanti. Il requirements.txt include l'elenco dei pacchetti Python installati, che saranno utilizzati dall'Esecuzione del Job quando vengono allegati.

Per questo laboratorio, abbiamo incluso Great Expectations, un framework popolare per la qualità e la validazione dei dati.

##### Creare un Job Spark CDE

Ora che sei familiare con i Repository e le Risorse CDE, sei pronto per creare il tuo primo Job Spark CDE.

Naviga alla scheda dei Job CDE e fai clic su "Crea un Job". Il lungo modulo caricato nella pagina ti consente di costruire uno Spark Submit come Job Spark CDE, passo dopo passo.

Inserisci i seguenti valori senza virgolette nei campi corrispondenti. Assicurati di aggiornare il nome utente con il tuo utente assegnato ovunque sia necessario:

```
* Tipo di Job: Spark
* Nome: 001_Lakehouse_Bronze_userxxx
* File: Seleziona dal Repository -> "cde_spark_jobs/001_Lakehouse_Bronze.py"
* Argomenti: userxxx #e.g. user002
* Opzioni Avanzate - Risorse: Spark_Files_Resource
* Opzioni Avanzate - Repository: CDE_Repo_userxxx e.g. CDE_Repo_user002
* Opzioni di Calcolo - aumenta "Nuclei dell'Executor" e "Memoria dell'Executor" da 1 a 2.
```

Infine, salva il Job CDE facendo clic sull'icona "Crea". ***Per favore, non selezionare "Crea ed Esegui".***

![alt text](../../img/new_spark

_job_1.png)

![alt text](../../img/new_spark_job_2.png)

Crea i due job rimanenti con i seguenti valori:

Job Lakehouse Silver:

```
* Tipo di Job: Spark
* Nome: 002_Lakehouse_Silver_userxxx
* File: Seleziona dal Repository -> "002_Lakehouse_Silver.py"
* Argomenti: userxxx #e.g. user002
* Opzioni Avanzate - Risorse: Spark_Files_Resource
* Opzioni Avanzate - Repository: CDE_Repo_userxxx e.g. CDE_Repo_user002
```

Job Lakehouse Gold:

```
* Tipo di Job: Spark
* Nome: 003_Lakehouse_Gold_userxxx
* File: Seleziona dal Repository -> "003_Lakehouse_Gold.py"
* Argomenti: userxxx #e.g. user002
* Opzioni Avanzate - Risorse: Spark_Files_Resource
* Opzioni Avanzate - Repository: CDE_Repo_userxxx e.g. CDE_Repo_user002
```

Di nuovo, ***per favore crea i job ma non eseguirli.***

![alt text](../../img/spark_job_create.png)


### Laboratorio 4: Orchestrare un pipeline Spark con Airflow

In questo laboratorio, costruirai un pipeline di Jobs Spark per caricare un nuovo batch di transazioni, unirli con i dati PII dei clienti e creare una tabella di clienti che potrebbero essere vittime di frode con carta di credito, includendo il loro indirizzo email e il loro nome. L'intero flusso di lavoro sarà orchestrato da Apache Airflow.

### Introduzione breve ad Airflow

Apache Airflow è una piattaforma per progettare, pianificare e eseguire pipeline di Data Engineering. È ampiamente utilizzato dalla comunità per creare flussi di lavoro dinamici e robusti per casi d'uso di Data Engineering in batch.

La caratteristica principale dei flussi di lavoro in Airflow è che tutti i flussi di lavoro sono definiti in codice Python. Il codice Python che definisce il flusso di lavoro viene memorizzato come una collezione di Task di Airflow organizzate in un DAG. Le attività sono definite tramite operatori integrati e moduli di Airflow. Gli operatori sono classi Python che possono essere istanziate per eseguire azioni predefinite e parametrizzate.

CDE integra Apache Airflow a livello del Cluster Virtuale CDE. Viene distribuito automaticamente per l'utente CDE quando viene creato il Cluster Virtuale CDE e non richiede manutenzione da parte dell'Amministratore CDE. Oltre agli operatori di base, CDE supporta il CDEJobRunOperator e il CDWOperator per attivare Jobs Spark e query di Datawarehousing.

##### Creare una Risorsa di File Airflow

Come con i Jobs Spark CDE, i job di Airflow possono sfruttare le Risorse di File CDE per caricare file, inclusi set di dati o parametri di esecuzione. Una Risorsa di File CDE denominata "Airflow_Files_Resource" che contiene il file "my_file.txt" è già stata creata per tutti i partecipanti.

##### Creare un Job Airflow

Apri lo script "004_airflow_dag.py" situato nella cartella "cde_airflow_jobs". Familiarizza con il codice e prendi nota di:

* Le classi Python necessarie per gli Operator DAG sono importate nella parte superiore. Nota che il CDEJobRunOperator è incluso per eseguire Jobs Spark in CDE.
* Il dizionario "default_args" include opzioni per la pianificazione, la definizione delle dipendenze e l'esecuzione generale.
* Tre istanze dell'oggetto CDEJobRunOperator sono dichiarate. Queste riflettono i tre Jobs Spark CDE che hai creato in precedenza.
* Infine, nella parte inferiore del DAG, sono dichiarate le Dipendenze delle Attività. Con questa dichiarazione, puoi specificare la sequenza di esecuzione delle attività del DAG.

Scarica il file da [questo URL](https://github.com/pdefusco/CDE_121_HOL/tree/main/cde_airflow_jobs) nella tua macchina locale. Aprilo nel tuo editor preferito e modifica la variabile utente nella riga 52.

Successivamente, naviga nell'interfaccia utente dei Jobs CDE e crea un nuovo Job CDE. Seleziona Airflow come Tipo di Job. Seleziona lo script "004_airflow_dag.py" e scegli di creare una nuova Risorsa di File nominata secondo le tue preferenze nel processo. Infine, aggiungi la dipendenza della Risorsa di File dove hai caricato "my_file.txt".

![alt text](../../img/new_airflow_job_1.png)

![alt text](../../img/new_airflow_job_2.png)

Monitora l'esecuzione del pipeline dall'interfaccia delle Esecuzioni dei Jobs. Nota che un Job Airflow verrà attivato e successivamente i tre Jobs Spark CDE verranno eseguiti uno per uno.

Mentre il job è in corso, apri l'interfaccia di Airflow e monitora l'esecuzione.

![alt text](../../img/new_airflow_job_3.png)


### Riepilogo

Cloudera Data Engineering (CDE) è un servizio serverless per la Cloudera Data Platform che ti consente di inviare job batch ai cluster virtuali con auto-scaling. CDE ti consente di dedicare più tempo alle tue applicazioni e meno tempo all'infrastruttura.

In questi laboratori, hai migliorato il tuo codice per renderlo riutilizzabile modulando la tua logica in funzioni e memorizzando queste funzioni come utilitari in una Risorsa di File CDE. Hai sfruttato la tua Risorsa di File memorizzando variabili dinamiche in un file di configurazione dei parametri e applicando una variabile nell'esecuzione attraverso il campo Argomenti. Come parte di pipeline Spark CI/CD più avanzate, il file dei parametri e il campo Argomenti possono essere sovrascritti e sostituiti durante l'esecuzione.

Hai poi utilizzato Apache Airflow non solo per orchestrare questi tre job, ma anche per eseguirli come parte di un pipeline di Data Engineering più complesso che toccava risorse in AWS. Grazie al vasto ecosistema di fornitori open source di Airflow, puoi anche operare su sistemi esterni e di terze parti.

Infine, hai eseguito il job e osservato le uscite nella pagina delle Esecuzioni dei Jobs CDE. CDE ha memorizzato le Esecuzioni dei Jobs, i log e le Risorse CDE associate per ogni esecuzione. Questo ti ha fornito capacità di monitoraggio e risoluzione dei problemi in tempo reale, nonché memorizzazione post-esecuzione di log, dipendenze di esecuzione e informazioni sul cluster. Esplorerai il Monitoraggio e l'Osservabilità in dettaglio nei prossimi laboratori.


### Link e risorse utili

* [Lavorare con le Risorse di File CDE](https://community.cloudera.com/t5/Community-Articles/Working-with-CDE-Files-Resources/ta-p/379891)
* [Monitoraggio efficace dei Jobs, delle Esecuzioni e delle Risorse con il CLI CDE](https://community.cloudera.com/t5/Community-Articles/Efficiently-Monitoring-Jobs-Runs-and-Resources-with-the-CDE/ta-p/379893)
* [Lavorare con i Parametri dei Job Spark CDE in Cloudera Data Engineering](https://community.cloudera.com/t5/Community-Articles/Working-with-CDE-Spark-Job-Parameters-in-Cloudera-Data/ta-p/380792)
* [Come analizzare file XML in CDE con il Pacchetto XML Spark](https://community.cloudera.com/t5/Community-Articles/How-to-parse-XMLs-in-Cloudera-Data-Engineering-with-the/ta-p/379451)
* [Spark Geospaziale con Apache Sedona in CDE](https://community.cloudera.com/t5/Community-Articles/Spark-Geospatial-with-Apache-Sedona-in-Cloudera-Data/ta-p/378086)
* [Automatizzare i Pipeline di Dati usando Apache Airflow in CDE](https://docs.cloudera.com/data-engineering/cloud/orchestrate-workflows/topics/cde-airflow-dag-pipeline.html)
* [Utilizzare CDE Airflow](https://github.com/pdefusco/Using_CDE_Airflow)
* [Documentazione sui Parametri dei DAG Airflow](https://airflow.apache.org/docs/apache-airflow/stable/tutorial.html#default-arguments)
* [Esplora l'Architettura Iceberg](https://github.com/pdefusco/Exploring_Iceberg_Architecture)
* [Qualità dei Dati Aziendali su Grande Scala in CDE con Great Expectations e i Runtimes Personalizzati CDE](https://community.cloudera.com/t5/Community-Articles/Enterprise-Data-Quality-at-Scale-with-Spark-and-Great/ta-p/378161)
