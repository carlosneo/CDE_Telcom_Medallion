# Despliegue y Orquestación de Jobs

* [Laboratorio 3: Crear recursos CDE y jobs Spark](https://github.com/pdefusco/CDE_121_HOL/blob/main/step_by_step_guides/spanish/part_02_deployment.md#lab-3-crear-recursos-cde-y-ejecutar-jobs-spark)
* [Laboratorio 4: Orquestar un pipeline Spark con Airflow](https://github.com/pdefusco/CDE_121_HOL/blob/main/step_by_step_guides/spanish/part_02_deployment.md#lab-4-orquestar-pipeline-spark-con-airflow)
* [Introducción breve a Apache Airflow](https://github.com/pdefusco/CDE_121_HOL/blob/main/step_by_step_guides/spanish/part_02_deployment.md#a-brief-introduction-to-airflow)
* [Resumen](https://github.com/pdefusco/CDE_121_HOL/blob/main/step_by_step_guides/spanish/part_02_deployment.md#summary)
* [Enlaces y recursos útiles](https://github.com/pdefusco/CDE_121_HOL/blob/main/step_by_step_guides/spanish/part_02_deployment.md#useful-links-and-resources)


### Laboratorio 3: Crear recursos CDE y jobs Spark

Hasta ahora, has utilizado sesiones para explorar datos de manera interactiva. CDE también te permite ejecutar código de aplicación Spark en modo batch como un Job CDE. Existen dos tipos de Jobs CDE: Spark y Airflow. En este laboratorio, vamos a crear un Job Airflow para orquestar tres Jobs Spark.

El Job CDE es una abstracción sobre el Spark Submit o el DAG Airflow. Con el Job Spark CDE, puedes crear una definición modular y reutilizable que se guarda en el clúster y puede ser modificada en la interfaz de usuario CDE (o a través del CLI y la API CDE) en cada ejecución. CDE almacena la definición del job para cada ejecución en la interfaz de Usuario de Ejecuciones de Jobs, por lo que puedes volver y hacer referencia a ella mucho después de la finalización de tu job.

Además, CDE te permite almacenar directamente artefactos como archivos Python, Jars y otras dependencias, o crear entornos Python y contenedores Docker en CDE como "Recursos CDE". Una vez creados en CDE, los Recursos están disponibles para los Jobs CDE como componentes modulares de la definición del Job CDE, que pueden ser intercambiados y referenciados por un job particular si es necesario.

Estas funcionalidades reducen considerablemente el esfuerzo necesario para gestionar y monitorear los Jobs Spark y Airflow. Al proporcionar un panel unificado para todas tus ejecuciones y una vista clara de todos los artefactos y dependencias asociados, CDE simplifica las operaciones de Spark & Airflow.

##### Familiarízate con el Código

Los scripts de aplicación Spark y los archivos de configuración utilizados en estos laboratorios están disponibles en la [carpeta de Jobs Spark CDE del repositorio git HOL](https://github.com/pdefusco/CDE_121_HOL/tree/main/cde_spark_jobs). Antes de pasar al siguiente paso, familiarízate con el código de los archivos "01_Lakehouse_Bronze.py", "002_Lakehouse_Silver.py", "003_Lakehouse_Gold.py", "utils.py", "parameters.conf".

El script DAG Airflow está disponible en la [carpeta de Jobs Airflow CDE del repositorio git HOL](https://github.com/pdefusco/CDE_121_HOL/tree/main/cde_airflow_jobs). Familiarízate también con el código del script "airflow_dag.py".

* El script "01_Lakehouse_Bronze.py" Aplicación PySpark crea tablas Iceberg para las transacciones de clientes y las tarjetas de crédito a partir de diferentes formatos de archivos. "utils.py" contiene un método Python para transformar varias columnas de dataframe a la vez, utilizado por el script "01_Lakehouse_Bronze.py".

* "parameters.conf" contiene una variable de configuración que se pasa a cada uno de los tres scripts PySpark. Almacenar variables en un Recurso de Archivos es un método comúnmente utilizado por los Ingenieros de Datos CDE para parametrizar dinámicamente scripts y pasar credenciales ocultas en la ejecución.

* "02_Lakehouse_Silver.py" carga los datos del nuevo archivo json de transacciones, los valida con Great Expectations y los añade a la tabla Transactions.

* "03_Lakehouse_Gold.py" carga los datos de la tabla Transactions filtrando por ID de snapshot Iceberg para reflejar solo el último lote. Luego, los une con la tabla de clientes y utiliza una UDF PySpark para filtrar a los clientes en función de la distancia desde el lugar de la transacción. Finalmente, crea una tabla de la capa Gold para proporcionar acceso curado a los analistas de negocios y otras partes interesadas autorizadas.

* "airflow_dag.py" orquesta el pipeline de Data Engineering. Primero, se crea un bucket AWS S3; se lee un archivo simple "my_file.txt" desde un Recurso de Archivos CDE y se escribe en el bucket S3. Sucesivamente, se ejecutan los tres Jobs Spark CDE discutidos anteriormente para crear una tabla de la capa Gold del Lakehouse.

##### Crear un Repositorio CDE

Los repositorios Git permiten a los equipos colaborar, gestionar artefactos del proyecto y promover aplicaciones del entorno de desarrollo al entorno de producción. CDE soporta la integración con proveedores de Git como GitHub, GitLab y Bitbucket para sincronizar ejecuciones de jobs con diferentes versiones de tu código.

En este punto, vas a crear un Repositorio CDE para clonar los scripts PySpark que contienen el Código de Aplicación para tu Job Spark CDE.

Desde la Página Principal, haz clic en "Repositorios" y luego en el ícono azul "Crear Repositorio".

![alt text](../../img/part3-repos-1.png)

Utiliza los siguientes parámetros para el formulario:

```
Nombre del Repositorio: CDE_Repo_userxxx
URL: https://github.com/pdefusco/CDE_121_HOL.git
Rama: main
```

![alt text](../../img/part3-repos-2.png)

Todos los archivos del repositorio git ahora están almacenados en CDE como un Repositorio CDE. Cada participante tendrá su propio repositorio CDE.

![alt text](../../img/part3-repos-3.png)

##### Crear un Recurso de Archivos CDE

Un recurso en CDE es una colección nombrada de archivos utilizados por un job o una sesión. Los recursos pueden incluir código de aplicación, archivos de configuración, imágenes Docker personalizadas y especificaciones de entorno virtual Python (requirements.txt). Los Ingenieros de Datos CDE explotan los Recursos de Archivos para almacenar archivos y otras dependencias de jobs en CDE, y luego asociarlos a las Ejecuciones de Jobs.

Un Recurso CDE de tipo "Archivos" que contiene los archivos "parameters.conf" y "utils.py" nombrado "Spark_Files_Resource" ya ha sido creado para todos los participantes.

##### Crear un Recurso de Entorno Python CDE

Un Recurso CDE de tipo "Python" construido con el archivo requirements.txt y nombrado "Python_Resource" ya ha sido creado para todos los participantes. El requirements.txt incluye la lista de paquetes Python instalados, que serán utilizados por la Ejecución del Job cuando se le adjunte.

Para este laboratorio, hemos incluido Great Expectations, un framework popular para la calidad y validación de datos.

##### Crear un Job Spark CDE

Ahora que estás familiarizado con los Repositorios y Recursos CDE, estás listo para crear tu primer Job Spark CDE.

Navega hasta la pestaña de Jobs CDE y haz clic en "Crear un Job". El largo formulario cargado en la página te permite construir un Spark Submit como Job Spark CDE, paso a paso.

Introduce los siguientes valores sin comillas en los campos correspondientes. Asegúrate de actualizar el nombre de usuario con tu usuario asignado en todas partes donde sea necesario:

```
* Tipo de Job: Spark
* Nombre: 001_Lakehouse_Bronze_userxxx
* Archivo: Seleccionar en el Repositorio -> "cde_spark_jobs/001_Lakehouse_Bronze.py"
* Argumentos: userxxx #e.g. user002
* Opciones Avanzadas - Recursos: Spark_Files_Resource
* Opciones Avanzadas - Repositorios: CDE_Repo_userxxx e.g. CDE_Repo_user002
* Opciones de Cálculo - aumenta "Núcleos del Executor" y "Memoria del Executor" de 1 a 2.
```

Finalmente, guarda el Job CDE haciendo clic en el ícono "Crear". ***Por favor, no selecciones "Crear y Ejecutar".***

![alt text](../../img/new_spark_job_1.png)

![alt text](../../img/new_spark_job_2.png)

Repite el proceso para los scripts PySpark restantes:

Job Lakehouse Silver:

```
* Tipo de Job: Spark
* Nombre: 002_Lakehouse_Silver_userxxx
* Archivo: Seleccionar en el Repositorio -> "002_Lakehouse_Silver.py"
* Argumentos: userxxx #e.g. user002
* Entorno Python: Python_Resource
* Opciones Avanzadas - Recursos: Spark_Files_Resource
* Opciones Avanzadas - Repositorios: CDE

_Repo_userxxx e.g. CDE_Repo_user002
```

Job Lakehouse Gold:

```
* Tipo de Job: Spark
* Nombre: 003_Lakehouse_Gold_userxxx
* Archivo: Seleccionar en el Repositorio -> "003_Lakehouse_Gold.py"
* Argumentos: userxxx #e.g. user002
* Opciones Avanzadas - Recursos: Spark_Files_Resource
* Opciones Avanzadas - Repositorios: CDE_Repo_userxxx e.g. CDE_Repo_user002
```

De nuevo, ***por favor crea los jobs pero no los ejecutes.***

![alt text](../../img/spark_job_create.png)


### Laboratorio 4: Orquestar un pipeline Spark con Airflow

En este laboratorio, construirás un pipeline de Jobs Spark para cargar un nuevo lote de transacciones, unirlos con los datos PII de los clientes, y crear una tabla de clientes que podrían ser víctimas de fraude con tarjeta de crédito, incluyendo su correo electrónico y su nombre. Todo el flujo de trabajo será orquestado por Apache Airflow.

### Introducción breve a Airflow

Apache Airflow es una plataforma para diseñar, planificar y ejecutar pipelines de Data Engineering. Es ampliamente utilizado por la comunidad para crear flujos de trabajo dinámicos y robustos para casos de uso de Data Engineering en batch.

La característica principal de los flujos de trabajo en Airflow es que todos los flujos de trabajo se definen en código Python. El código Python que define el flujo de trabajo se almacena como una colección de Tareas Airflow organizadas en un DAG. Las tareas se definen mediante operadores integrados y módulos de Airflow. Los operadores son clases Python que pueden ser instanciadas para realizar acciones predefinidas y parametrizadas.

CDE integra Apache Airflow a nivel del Clúster Virtual CDE. Se despliega automáticamente para el usuario CDE al crear el Clúster Virtual CDE y no requiere mantenimiento por parte del Administrador CDE. Además de los operadores básicos, CDE admite el CDEJobRunOperator y el CDWOperator para desencadenar Jobs Spark y consultas de Datawarehousing.

##### Crear un Recurso de Archivos Airflow

Al igual que con los Jobs Spark CDE, los jobs de Airflow pueden aprovechar los Recursos de Archivos CDE para cargar archivos, incluidos conjuntos de datos o parámetros de ejecución. Un Recurso de Archivos CDE denominado "Airflow_Files_Resource" que contiene el archivo "my_file.txt" ya ha sido creado para todos los participantes.

##### Crear un Job Airflow

Abre el script "004_airflow_dag.py" ubicado en la carpeta "cde_airflow_jobs". Familiarízate con el código y toma nota de:

* Las clases Python necesarias para los DAG Operators están importadas en la parte superior. Ten en cuenta que el CDEJobRunOperator está incluido para ejecutar Jobs Spark en CDE.
* El diccionario "default_args" incluye opciones para la planificación, la definición de dependencias y la ejecución general.
* Tres instancias del objeto CDEJobRunOperator están declaradas. Estas reflejan los tres Jobs Spark CDE que has creado anteriormente.
* Finalmente, en la parte inferior del DAG, se declaran las Dependencias de las Tareas. Con esta declaración, puedes especificar la secuencia de ejecución de las tareas del DAG.

Descarga el archivo desde [esta URL](https://github.com/pdefusco/CDE_121_HOL/tree/main/cde_airflow_jobs) en tu máquina local. Ábrelo en tu editor de tu elección y modifica la variable de usuario en la línea 52.

Luego, navega a la interfaz de usuario de los Jobs CDE y crea un nuevo Job CDE. Selecciona Airflow como Tipo de Job. Selecciona el script "004_airflow_dag.py" y elige crear un nuevo Recurso de Archivos nombrado según tu preferencia en el proceso. Finalmente, agrega la dependencia del Recurso de Archivos donde cargaste "my_file.txt".

![alt text](../../img/new_airflow_job_1.png)

![alt text](../../img/new_airflow_job_2.png)

Monitorea la ejecución del pipeline desde la interfaz de Ejecuciones de Jobs. Nota que un Job Airflow será desencadenado y sucesivamente los tres Jobs Spark CDE se ejecutarán uno por uno.

Mientras el job está en curso, abre la interfaz de Airflow y supervisa la ejecución.

![alt text](../../img/new_airflow_job_3.png)


### Resumen

Cloudera Data Engineering (CDE) es un servicio sin servidor para la Cloudera Data Platform que te permite enviar jobs batch a clústeres virtuales con auto-escalado. CDE te permite pasar más tiempo en tus aplicaciones y menos tiempo en la infraestructura.

En estos laboratorios, has mejorado tu código para hacerlo reutilizable modulando tu lógica en funciones, y almacenando estas funciones como utilitarios en un Recurso de Archivos CDE. Has aprovechado tu Recurso de Archivos al almacenar variables dinámicas en un archivo de configuración de parámetros y aplicando una variable en la ejecución a través del campo Argumentos. Como parte de pipelines Spark CI/CD más avanzados, el archivo de parámetros y el campo Argumentos pueden ser sobrescritos y reemplazados en la ejecución.

Luego has utilizado Apache Airflow no solo para orquestar estos tres jobs, sino también para ejecutarlos como parte de un pipeline de Data Engineering más complejo que tocaba recursos en AWS. Gracias al vasto ecosistema de proveedores open source de Airflow, también puedes operar en sistemas externos y de terceros.

Finalmente, has ejecutado el job y observado las salidas en la página de Ejecuciones de Jobs CDE. CDE ha almacenado las Ejecuciones de Jobs, los registros y los Recursos CDE asociados para cada ejecución. Esto te ha proporcionado capacidades de monitoreo y resolución de problemas en tiempo real, así como almacenamiento post-ejecución de registros, dependencias de ejecución e información sobre el clúster. Explorarás la Monitoreo y Observabilidad en detalle en los próximos laboratorios.


### Enlaces y recursos útiles

* [Trabajar con Recursos de Archivos CDE](https://community.cloudera.com/t5/Community-Articles/Working-with-CDE-Files-Resources/ta-p/379891)
* [Monitoreo eficaz de Jobs, Ejecuciones y Recursos con el CLI CDE](https://community.cloudera.com/t5/Community-Articles/Efficiently-Monitoring-Jobs-Runs-and-Resources-with-the-CDE/ta-p/379893)
* [Trabajar con Parámetros de Job Spark CDE en Cloudera Data Engineering](https://community.cloudera.com/t5/Community-Articles/Working-with-CDE-Spark-Job-Parameters-in-Cloudera-Data/ta-p/380792)
* [Cómo analizar archivos XML en CDE con el Paquete XML Spark](https://community.cloudera.com/t5/Community-Articles/How-to-parse-XMLs-in-Cloudera-Data-Engineering-with-the/ta-p/379451)
* [Spark Geoespacial con Apache Sedona en CDE](https://community.cloudera.com/t5/Community-Articles/Spark-Geospatial-with-Apache-Sedona-in-Cloudera-Data/ta-p/378086)
* [Automatizar Pipelines de Datos usando Apache Airflow en CDE](https://docs.cloudera.com/data-engineering/cloud/orchestrate-workflows/topics/cde-airflow-dag-pipeline.html)
* [Usar CDE Airflow](https://github.com/pdefusco/Using_CDE_Airflow)
* [Documentación sobre los Argumentos de los DAG Airflow](https://airflow.apache.org/docs/apache-airflow/stable/tutorial.html#default-arguments)
* [Explorar la Arquitectura Iceberg](https://github.com/pdefusco/Exploring_Iceberg_Architecture)
* [Calidad de Datos Empresariales a Gran Escala en CDE con Great Expectations y los Runtimes Personalizados CDE](https://community.cloudera.com/t5/Community-Articles/Enterprise-Data-Quality-at-Scale-with-Spark-and-Great/ta-p/378161)
