# Lecciones aprendidas

Este documento recoge los problemas reales con los que me encontré construyendo este proyecto — de datos y de infraestructura — junto con cómo los diagnostiqué y los resolví. Lo escribo para dejar constancia del razonamiento detrás de cada decisión, no solo del resultado final.

---

## Parte 1 — Calidad e integridad de los datos

### 1.1 Schema drift entre años

Cuando comparé el schema de los archivos de enero de 2023, 2024 y 2025, encontré tres diferencias que no estaban documentadas en ningún sitio evidente:

| Cambio | Detalle |
|---|---|
| Cambio de tipo | `VendorID`, `passenger_count`, `RatecodeID`, `PULocationID`, `DOLocationID` pasan de `double`/`long` a tipos más específicos entre 2023 y 2024 |
| Rename silencioso | `airport_fee` (2023) → `Airport_fee` (2024+), solo cambia la capitalización |
| Columna nueva | `cbd_congestion_fee` aparece recién en 2025 (tarifa de congestión del distrito central, vigente desde enero de ese año) |

Si no lo hubiera detectado a tiempo, un job de transformación escrito asumiendo el schema de un solo año habría fallado al toparse con otro, o peor, habría producido archivos de `silver` con schemas distintos entre sí, rompiendo la idea de que una capa "limpia" debe ser homogénea.

Para resolverlo escribí una función `normalize_schema()` que renombra columnas a una convención `snake_case` fija, rellena columnas ausentes con `null`, aplica *casts* de forma idempotente (no falla si la columna ya tenía el tipo correcto) y fuerza un orden canónico de columnas con `.select()` explícito, de forma que cualquier columna nueva no anticipada me falle con un error claro en vez de colarse sin que me diera cuenta.

### 1.2 Lo que pensé que eran duplicados y en realidad eran reversiones financieras

Cuando estaba evaluando la deduplicación en `silver`, probé una clave de negocio parcial (vendor + timestamps + ubicación + tarifa base) y me marcó 165 filas como "duplicadas". Antes de aplicar `dropDuplicates()` con esa clave, decidí inspeccionar los pares para entender qué eran.

Encontré que tenían el mismo vendor, los mismos timestamps, la misma distancia y la misma tarifa base, pero los recargos (`mta_tax`, `congestion_surcharge`, `total_amount`, etc.) tenían **signo invertido** entre ambas filas, cancelándose entre sí.

No era ruido ni un error de carga: es un patrón conocido de NYC TLC que representa una transacción y su ajuste o reversión. Si hubiera aplicado la deduplicación que tenía planeada, habría **eliminado datos financieros legítimos**.

Para confirmarlo del todo, probé `dropDuplicates()` sin argumentos (fila exacta completa) y me dio **0 duplicados reales**. Con eso concluí que no había nada que deduplicar: mi clave de negocio estaba mal diseñada, no los datos.

Al final me quedé con `dropDuplicates()` sin argumentos, como salvaguarda de idempotencia, sin ninguna lógica de negocio adicional que pudiera destruir información real.

### 1.3 El patrón del vendor 6

El checkpoint de calidad (`validate_silver`) me detectó, de forma consistente en 35 de 36 meses, filas donde `tpep_dropoff_datetime` era anterior a `tpep_pickup_datetime` por unos pocos segundos, nunca minutos ni horas.

Cuando agrupé por `vendor_id`, encontré que 65 de 67 casos en un mes de muestra correspondían a un único vendor. Mi hipótesis más plausible es que ese proveedor registra el timestamp de cierre o procesamiento del viaje en su backend, en vez del momento cronológico exacto: un desfase sistemático de la fuente, no un error de mi pipeline.

Decidí documentar el hallazgo y dejar que la validación lo reporte (`logger.warning`) sin bloquear el pipeline, en vez de intentar "corregir" un timestamp cuyo valor real no conozco.

### 1.4 Registros con fechas antiguas

Cuando construí las agregaciones diarias de `gold`, me aparecieron filas en el archivo de "enero 2023" con fechas de 2008 y 2022: datos corruptos o mal etiquetados en la fuente que ninguna capa anterior había filtrado, porque la validación de silver solo comprobaba fechas *futuras*, no un mínimo razonable.

Lo resolví filtrando explícitamente por `year`/`month` del dataset dentro del job de agregación, antes de agrupar.

Dejo pendiente, y sin resolver todavía, agregar un check de "fecha mínima razonable" a `validate_silver` — lo anoto aquí para no perderlo de vista.

### 1.5 `timestamp` vs `timestamp_ntz`

Las columnas de fecha del dataset son `timestamp_ntz` (sin zona horaria). Tanto en mi primer intento con Great Expectations como después con Pandera, declarar el tipo como `timestamp`/`TimestampType` genérico me causó errores de comparación: el motor esperaba un tipo con timezone y encontraba uno sin. Lo corregí declarando explícitamente `TimestampNTZType` (en Pandera) y usando un `datetime.now()` *naive* (sin `tzinfo`) para las comparaciones de "no fechas futuras".

---

## Parte 2 — Great Expectations vs. Pandera

Implementé el checkpoint de calidad de silver primero con Great Expectations 1.19.1. Tras varias horas de fricción (errores de `DataContext` no inicializado, `ExpectationSuite`/`ValidationDefinition` que exigían registro explícito en un Store, y finalmente un `ResourceFreshnessAggregateError` con el mensaje *"Could not find datasource"*), investigué el error y encontré que correspondía a un bug reportado por el propio equipo de GX en su changelog oficial, relacionado con el chequeo de "freshness" en contextos efímeros.

Confirmé después que ese bug específico ya aparece marcado como corregido en el changelog de una versión posterior a la que yo estaba usando — es decir, no era un problema irresoluble de la librería, sino algo que estaba activo en la versión que instalé en ese momento. Aun así, decidí migrar a **Pandera** porque para mi caso de uso (validar un DataFrame de Spark sin necesidad de persistir nada) el modelo de Pandera es bastante más simple: un `DataFrameSchema` se valida directamente contra el DataFrame, sin contexto, sin stores, sin registro de suites.

Los checks de rango de filas y de orden pickup/dropoff los terminé implementando con Spark nativo (filtros y conteos) en vez de forzar la API de checks de Pandera, que también mostró limitaciones propias con columnas `timestamp_ntz`.

---

## Parte 3 — Airflow en Docker: la cadena de fallos de infraestructura

Ejecutar el pipeline de PySpark dentro de contenedores de Airflow, en vez de invocarlo manualmente desde mi máquina, me expuso una cadena de problemas de infraestructura, cada uno con una causa concreta que tuve que aislar por separado:

| # | Lo que veía | Por qué pasaba | Cómo lo resolví |
|---|---|---|---|
| 1 | El healthcheck del scheduler siempre en `unhealthy` | Es un bug conocido de Airflow: el compose oficial de la serie 3.x todavía trae el healthcheck de 2.x (`curl :8974`) | Lo reemplacé por `airflow jobs check --job-type SchedulerJob` |
| 2 | No podía iniciar sesión con credenciales fijas | `AIRFLOW__SIMPLE_AUTH_MANAGER__USERNAME/PASSWORD` no son claves de configuración reales; el Simple Auth Manager siempre autogenera la contraseña | Empecé a leer la contraseña generada desde el log del `api-server` |
| 3 | `ModuleNotFoundError` al importar PySpark desde el DAG | El proceso de la task de Airflow usa su propio intérprete (`/home/airflow/.local`), no el `PATH` de mi shell | Instalé el proyecto directamente en ese entorno (`pip install --user .`) en vez de depender de un `.venv` externo |
| 4 | `Connection refused` al ejecutar cualquier task | Me faltaba `AIRFLOW__CORE__EXECUTION_API_SERVER_URL`; el proceso de la task no sabía cómo alcanzar el API server en la red de Docker | Seteé la URL explícita apuntando al nombre del servicio (`http://airflow-api-server:8080/execution/`) |
| 5 | `PermissionError` al crear la carpeta de logs de mi app | La ruta relativa `logs/` no era escribible por el usuario `airflow` dentro del contenedor | Redirigí `LOG_DIRECTORY` a una ruta absoluta dentro del volumen de logs de Airflow, solo para ese entorno |
| 6 | `ModuleNotFoundError: nyc_taxi_lakehouse.config` después de instalar el paquete | Tenía el patrón `config/` en `.gitignore`/`.dockerignore` sin ancla de raíz (`/config/`), y eso excluía también `src/nyc_taxi_lakehouse/config/` del build, porque mi build backend (`hatchling`) respeta `.gitignore` al empaquetar | Anclé el patrón a la raíz (`/config/`) en ambos archivos |
| 7 | 19 errores de validación de Pydantic Settings dentro del contenedor | Calculaba la ruta del `.env` de forma relativa a la ubicación del propio `settings.py`, lo cual funcionaba en desarrollo pero se rompía una vez el paquete quedaba instalado en `site-packages` | Permití sobreescribir la ruta vía variable de entorno (`ENV_FILE_PATH`), dejando el cálculo relativo solo como *fallback* para desarrollo local |
| 8 | No lograba conectar a MinIO desde dentro de los contenedores de Airflow | `MINIO_ENDPOINT=localhost:9000` en mi `.env` es correcto para el host, pero dentro de un contenedor `localhost` se refiere al contenedor mismo | Sobreescribí `MINIO_ENDPOINT=minio:9000` solo en el `environment` de los servicios de Airflow |
| 9 | Me faltaban carpetas del paquete después de `pip install .` | `spark/`, `spark/jobs/`, `spark/quality/` y `spark/transformations/` no tenían `__init__.py`; en modo desarrollo funcionaba igual (namespace packages implícitos vía `uv run`), pero no al construir un wheel real | Agregué `__init__.py` a cada subpaquete |

Lo que más me quedó claro de esta parte: que un pipeline funcione perfecto ejecutado a mano desde mi máquina no me garantiza nada sobre cómo se va a comportar orquestado dentro de contenedores. La diferencia de entorno (red, sistema de archivos, usuario, intérprete de Python, cómo se empaqueta el código) mete una categoría de fallos completamente distinta a los bugs de lógica de negocio, y cada uno lo tuve que aislar por separado en vez de asumir que sabía cuál era la causa.