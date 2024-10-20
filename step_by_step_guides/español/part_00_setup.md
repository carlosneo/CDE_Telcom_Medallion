# Setup Automático

## Objetivo

Este repositorio de git aloja la automatización de la configuración para el HOL. El script consta de una serie de comandos CDE CLI que primero crean datos sintéticos en el almacenamiento en la nube para cada participante y luego crean el Recurso CDE compartido por cada participante.

Esta automatización no ejecuta el material real del laboratorio. Estos son fáciles de crear desde la interfaz de usuario de CDE y no requieren automatización.

## Contenido

* [Requisitos](https://github.com/pdefusco/CDE_121_HOL/blob/main/step_by_step_guides/spanish/part_00_setup.md#requisitos)
* [Información Importante](https://github.com/pdefusco/CDE_121_HOL/blob/main/step_by_step_guides/spanish/part_00_setup.md#información-importante)
* [Opciones de Implementación](https://github.com/pdefusco/CDE_121_HOL/blob/main/step_by_step_guides/spanish/part_00_setup.md#opciones-de-implementación)
  * [1. Instrucciones de Implementación con Docker](https://github.com/pdefusco/CDE_121_HOL/blob/main/step_by_step_guides/spanish/part_00_setup.md#1-instrucciones-de-implementación-con-docker)
  * [2. Instrucciones de Implementación Local](https://github.com/pdefusco/CDE_121_HOL/blob/main/step_by_step_guides/spanish/part_00_setup.md#2-instrucciones-de-implementación-local)
* [Instrucciones de Desmontaje](https://github.com/pdefusco/CDE_121_HOL/blob/main/step_by_step_guides/spanish/part_00_setup.md#instrucciones-de-desmontaje)
* [Resumen](https://github.com/pdefusco/CDE_121_HOL/blob/main/step_by_step_guides/spanish/part_00_setup.md#resumen)

## Requisitos

Para implementar la demostración mediante esta automatización necesitas:

* Un inquilino de CDP en la nube pública o privada.
* Un Usuario de CDE Workload con políticas de Ranger y Mappings de IDBroker configurados en consecuencia.
* Un Servicio CDE en versión 1.21 o superior.
* La autorización de Runtime Personalizado de Docker. Por favor, contacta al equipo de producto o ventas de CDE para obtener la autorización.
* Una cuenta de Dockerhub. Por favor, ten lista tu cuenta y contraseña de Dockerhub.

## Información Importante

La automatización despliega lo siguiente en tu Clúster Virtual de CDE:

* Un Trabajo de Spark de CDE y Recursos asociados de CDE con el propósito de crear datos sintéticos en el Almacenamiento en la Nube para cada participante.
* Un Recurso de Archivos de CDE para archivos de Spark compartidos por todos los participantes llamado "Spark-Files-Shared".
* Un Recurso de Archivos de CDE para archivos de Airflow compartidos por todos los participantes llamado "Airflow-Files-Shared".
* Un Recurso de Python de CDE compartido por todos los participantes llamado "Python-Env-Shared".

## Opciones de Implementación

Hay dos opciones de implementación: Docker o clonando este repositorio de git y ejecutando los scripts de configuración localmente. Se recomienda la opción Docker. Por favor, sigue uno de los dos conjuntos de instrucciones a continuación.

Cuando la configuración esté completa, navega a la interfaz de usuario de CDE y valida que la ejecución del trabajo se haya completado con éxito. Esto implica que los datos del HOL se hayan creado con éxito en el Almacenamiento en la Nube.

### 1. Instrucciones de Implementación con Docker

Se ha creado un contenedor Docker con todas las dependencias necesarias para la implementación del HOL.

```
% docker run -it pauldefusco/cde121holsetup
```

En el shell del contenedor, modifica la URL de la API de Jobs en la configuración de CDE CLI:

```
[cdeuser@1234567 ~]$ vi ~/.cde/config.yaml

user: <cdp_workload_username>
vcluster-endpoint: https://a1b2345.cde-abcdefg.cde-xxx.xxxxx.cloudera.site/dex/api/v1
```

Luego ejecuta el script de implementación con:

```
[cdeuser@1234567 ~]$ ./setup/deploy_hol.sh <docker-user> <cdp-workload-user> <number-of-participants> <storage-location>
```

Por ejemplo:

```
#AWS
[cdeuser@1234567 ~]$ ./setup/deploy_hol.sh pauldefusco pauldefusco 3 s3a://goes-se-sandbox01/data
```

```
#Azure
[cdeuser@1234567 ~]$ ./setup/deploy_hol.sh pauldefusco pauldefusco 3 abfs://logs@go01demoazure.dfs.core.windows.net/data
```

### 2. Instrucciones de Implementación Local

Clona este repositorio en tu máquina. Luego ejecuta el script de implementación con:

```
% ./setup/deploy_hol.sh <docker-user> <cdp-workload-user> <number-of-participants> <storage-location>
```

Por ejemplo:

```
#AWS
% ./setup/deploy_hol.sh pauldefusco pauldefusco 3 s3a://goes-se-sandbox01/data
```

```
#Azure
% ./setup/deploy_hol.sh pauldefusco pauldefusco 3 abfs://logs@go01demoazure.dfs.core.windows.net/data
```

## Instrucciones de Desmontaje

Cuando termines, ejecuta este script para desmontar los datos en el Catálogo pero no en S3. Ese paso será manejado por los scripts de desmontaje de GOES.

```
% ./teardown.sh cdpworkloaduser
```

## Resumen

Puedes implementar una demostración completa de CDE con la automatización proporcionada. La demostración ejecuta un pequeño pipeline ETL que incluye Iceberg, Spark y Airflow.
