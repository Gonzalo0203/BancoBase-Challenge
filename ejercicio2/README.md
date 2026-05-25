# Ejercicio 2 - Propuesta de Arquitectura de Datos

## 1. Contexto del problema

El objetivo del ejercicio es proponer una arquitectura de datos para consolidar información proveniente de tres fuentes transaccionales que operan 24/7:

| Fuente | Tipo | Descripción |
|---|---|---|
| F1 | CRM propietario | Información de clientes, datos demográficos y datos de contacto |
| F2 | SQL Server | Transacciones de clientes sobre una parte de los productos |
| F3 | PostgreSQL | Transacciones de clientes sobre el resto de los productos |

La arquitectura debe cumplir con dos objetivos principales:

1. Habilitar al área operativa para realizar consultas SQL.
2. Permitir al equipo de ciencia de datos aplicar algoritmos de detección de patrones como clustering o búsquedas en grafos.

---

## 2. Arquitectura propuesta

Se propone una arquitectura tipo **Lakehouse**, complementada con una capa de consulta SQL para usuarios operativos y una capa especializada para ciencia de datos.

La solución considera ingesta incremental, almacenamiento por capas, transformación de datos, control de calidad, gobierno, seguridad y exposición de datos para distintos consumidores.

---

## 3. Diagrama de arquitectura propuesta

![Arquitectura Propuesta](./dataArchitectureEjercicio2.svg)


---

## 4. Herramientas que se podrían usar por etapa

| Etapa | Herramientas que se podrían usar | Uso |
|---|---|---|
| Ingesta desde CRM propietario | API REST, Airbyte, Python, conector custom | Extraer datos de clientes desde una fuente propietaria |
| Ingesta desde SQL Server | AWS DMS, Azure Data Factory, Airbyte, JDBC | Extraer transacciones de SQL Server con bajo impacto |
| Ingesta desde PostgreSQL | AWS DMS, Airbyte, JDBC, logical replication | Extraer transacciones desde PostgreSQL |
| Orquestación | Apache Airflow, Cloud Composer, Azure Data Factory | Coordinar tareas, dependencias y horarios de ejecución |
| Almacenamiento Raw | Amazon S3, Azure Data Lake Storage, Google Cloud Storage, MinIO | Guardar datos originales sin modificar |
| Formato analítico (Bronze) | Parquet, Delta Lake, Apache Iceberg | Optimizar lectura, compresión y evolución de datos |
| Procesamiento | Apache Spark, Databricks, AWS Glue, PySpark, Python | Limpieza, integración y transformación de datos |
| Transformación (Silver) | dbt, Spark SQL, Trino, Snowflake SQL, BigQuery SQL | Modelado analítico y reglas de negocio |
| Calidad de datos | dbt tests | Validar nulos, duplicados, rangos y reglas |
| Data Warehouse (Gold) | Snowflake, Amazon Redshift, BigQuery, Azure Synapse | Consultas SQL para usuarios operativos |
| Consulta sobre Data Lake | Trino, Athena, Databricks SQL | Consulta SQL directa sobre archivos en el Data Lake |
| Ciencia de datos | Databricks, SageMaker, Vertex AI, notebooks | Preparación de datasets y entrenamiento de modelos |
| Grafos | Neo4j, Amazon Neptune, TigerGraph | Análisis de relaciones entre clientes, productos y transacciones |
| Catálogo y metadata | Glue Data Catalog, DataHub, OpenMetadata, Apache Atlas | Control de metadatos, linaje y documentación |
| Seguridad | IAM, KMS, Secrets Manager, Vault, RBAC | Control de accesos, cifrado y secretos |

---

# 5. Respuestas a preguntas

## A. ¿Qué subconjunto de cada fuente extraería?

No extraería toda la información de las fuentes. Extraería únicamente los campos necesarios para cumplir los dos objetivos: consultas SQL operativas y análisis de ciencia de datos. Para ello definiría con negocio los datos que necesitan, en caso de que sea la mayoría o no supieran los datos que necesitan extraería todo pero en los catálogos de silver y gold solo definiría lo necesario.

### F1 - CRM propietario

Del CRM extraería información de clientes, datos demográficos y algunos datos de contacto necesarios para operación.

Ejemplo de campos:

| Campo | Uso |
|---|---|
| `customer_id` | Identificador del cliente |
| `name` | Nombre del cliente o razón social |
| `birth_date` | Cálculo de edad o rango de edad |
| `gender` | Variable demográfica |
| `customer_segment` | Segmentación comercial |
| `email` | Contacto operativo, con protección de PII |
| `phone` | Contacto operativo, con protección de PII |
| `address` | Ubicación o zona, con protección de PII |
| `customer_status` | Cliente activo, inactivo o bloqueado |
| `created_at` | Fecha de alta |
| `updated_at` | Control incremental |

Para ciencia de datos, no necesariamente usaría el dato sensible directo como correo o teléfono. En su lugar usaría atributos derivados, enmascarados o agregados, por correo, zona geográfica o rango de edad.

### F2 - SQL Server

De SQL Server extraería las transacciones de una parte de los productos.

Ejemplo de campos:

| Campo | Uso |
|---|---|
| `transaction_id` | Identificador de la transacción |
| `customer_id` | Relación con cliente |
| `product_id` | Relación con producto |
| `product_type` | Clasificación del producto |
| `amount` | Monto de la transacción |
| `currency` | Moneda |
| `transaction_date` | Fecha de operación |
| `transaction_status` | Estado de la transacción |
| `channel` | Canal de operación |
| `branch_id` | Sucursal o punto de atención |
| `updated_at` | Control incremental |

### F3 - PostgreSQL

De PostgreSQL extraería campos similares a SQL Server para poder homologar las transacciones.

Ejemplo de campos:

| Campo | Uso |
|---|---|
| `transaction_id` | Identificador de la transacción |
| `customer_id` | Relación con cliente |
| `product_id` | Relación con producto |
| `product_type` | Clasificación del producto |
| `amount` | Monto de la transacción |
| `currency` | Moneda |
| `transaction_date` | Fecha de operación |
| `transaction_status` | Estado de la transacción |
| `channel` | Canal de operación |
| `branch_id` | Sucursal o punto de atención |
| `updated_at` | Control incremental |

Además, agregaría un campo técnico llamado `source_system` para identificar el origen de cada registro

## B. ¿Qué posibles retos implica la extracción de cada fuente y qué herramientas utilizarías?

### F1 - CRM propietario

Al ser un CRM propietario, el principal reto es que posiblemente no permita una conexión directa a base de datos. Puede requerir consumo por API, exportación de archivos o conectores específicos.

Retos principales:

- Límites de consumo de API.
- Paginación.
- Cambios en estructura de respuesta.
- Autenticación mediante tokens.
- Datos sensibles de clientes.
- Posible falta de campos de control incremental.

Herramientas sugeridas:

- API REST del CRM.
- Airbyte o Fivetran si existe conector.
- Python para un conector custom.
- Airflow para orquestar la extracción.
- Secrets Manager o Vault para almacenar credenciales.
- S3, ADLS o GCS para guardar la extracción cruda.

### F2 - SQL Server

Al ser una base transaccional 24/7, el reto es extraer información sin afectar las operaciones del sistema.

Retos principales:

- Bloqueos en tablas transaccionales.
- Alto volumen de transacciones.
- Consultas pesadas.
- Necesidad de extracción incremental.
- Tipos de datos propietarios o formatos específicos.

Herramientas sugeridas:

- SQL Server CDC.
- Debezium.
- AWS DMS.
- Azure Data Factory.
- Réplica de lectura.
- Conector JDBC.
- Airflow para orquestación.

### F3 - PostgreSQL

PostgreSQL también requiere una estrategia incremental para no consultar toda la tabla diariamente.

Retos principales:

- Control de cambios.
- Extracciones por ventanas de tiempo.
- Lectura de WAL o logical replication.
- Índices necesarios para consultas incrementales.
- Posibles diferencias de tipos de datos frente a SQL Server.

Herramientas sugeridas:

- Logical replication.
- Debezium.
- AWS DMS.
- Airbyte.
- Conector JDBC.
- Airflow para orquestación.

---

## C. ¿Qué posibles retos implica la independencia en el modelo de datos de las tres fuentes y cómo los resolverías?

El reto principal es que las tres fuentes fueron diseñadas de forma independiente. Esto puede generar diferencias en nombres de columnas, tipos de datos, llaves, catálogos, reglas de negocio y definición de conceptos.

Ejemplos de problemas:

- El cliente puede tener identificadores distintos en cada sistema.
- Un producto puede tener códigos diferentes en F2 y F3.
- Los estatus de transacción pueden no llamarse igual.
- Las fechas pueden tener formatos o zonas horarias distintas.
- Los montos pueden venir en diferentes monedas.
- Una misma entidad puede tener distintos nombres de campos.

Para resolverlo, propondría crear un **modelo canónico**.

Ejemplo de modelo canónico para transacciones:

| Campo canónico | Descripción |
|---|---|
| `global_transaction_id` | Identificador único global |
| `source_transaction_id` | Identificador original |
| `global_customer_id` | Identificador homologado de cliente |
| `source_customer_id` | Identificador original del cliente |
| `product_id` | Producto homologado |
| `source_product_id` | Producto original |
| `source_system` | Sistema origen |
| `transaction_date` | Fecha estándar |
| `amount` | Monto normalizado |
| `currency` | Moneda |
| `status` | Estatus homologado |
| `channel` | Canal homologado |

También usaría tablas de homologación:

```text
dim_customer_cross_reference
dim_product_cross_reference
dim_status_catalog
dim_channel_catalog
```

Estas tablas permitirían mapear valores originales a valores estándar.

---

## D. Aparte de un proceso batch en la hora de menor uso, ¿cómo mitigarías el impacto del pipeline sobre las fuentes originales?

Para mitigar el impacto sobre sistemas transaccionales, usaría estrategias de extracción incremental y desacoplamiento.

Opciones recomendadas:

1. **CDC - Change Data Capture**

   Permite capturar cambios sin hacer lecturas completas de las tablas.

   Herramientas Posibles:
   - AWS DMS.
   - SQL Server CDC.
   - PostgreSQL logical replication.

2. **Réplicas de lectura**

   En lugar de consultar la base productiva, el pipeline lee desde una réplica.

   Herramientas Posibles:
   - Read replicas.
   - PostgreSQL replicas.

3. **Extracción incremental por columna de actualización**

   Si existen columnas como `updated_at`, se pueden extraer únicamente los registros nuevos o modificados.

4. **Paginación y ventanas de tiempo**

   Dividir la extracción en lotes pequeños reduce el impacto.

5. **Índices sobre columnas de extracción**

   Crear índices en campos como `updated_at`, `transaction_date` o llaves primarias ayuda a reducir el costo de las consultas.

6. **Colas o eventos**

   Cuando sea posible, publicar cambios a una cola o stream.

   Herramientas Posibles:
   - Kafka.
   - Amazon MSK.
   - EventBridge.
   - Pub/Sub.
   - Azure Event Hubs.

---

## E. ¿Cuáles etapas considerarías en tu proceso de transformación de datos y qué uso les darías?

Usaría una arquitectura por capas.

### 1. Raw / Landing

Guarda los datos tal como llegan desde la fuente.

Uso:

- Auditoría.
- Reproceso.
- Trazabilidad.
- Conservación de datos originales.

Herramientas:

- Amazon S3.
- Azure Data Lake Storage.
- Google Cloud Storage.
- MinIO.
- Formatos JSON, CSV, Avro o Parquet crudo.

### 2. Bronze

Primera estandarización de datos.

Uso:

- Conversión de tipos.
- Limpieza mínima.
- Normalización de nombres de columnas.
- Agregar columnas técnicas como `source_system`, `ingestion_date` y `batch_id`.

Herramientas:

- Spark.
- Databricks.
- AWS Glue.
- PySpark.
- Polars para volúmenes pequeños o medianos.

### 3. Silver

Capa integrada y homologada.

Uso:

- Unificar clientes.
- Homologar productos.
- Deduplicar registros.
- Validar reglas de calidad.
- Resolver diferencias entre SQL Server y PostgreSQL.

Herramientas:

- Spark.
- dbt.
- Databricks.
- Delta Lake o Iceberg.

### 4. Gold

Capa lista para consumo de negocio.

Uso:

- Data marts.
- Indicadores operativos.
- KPIs.
- Tablas agregadas.
- Modelos dimensionales.

Herramientas:

- dbt.
- Snowflake.
- Redshift.
- BigQuery.
- Synapse.
- Trino.

### 5. Feature / Graph Layer

Capa enfocada en ciencia de datos.

Uso:

- Variables para clustering.
- Relaciones cliente-producto.
- Redes de clientes y productos.
- Datasets para modelos.

Herramientas:

- Feature Store.
- Databricks Feature Store.
- SageMaker Feature Store.
- Neo4j.
- Amazon Neptune.
- TigerGraph.

---

## F. ¿Qué herramientas utilizas para las etapas de transformación?

Utilizaría distintas herramientas dependiendo del volumen, criticidad y tipo de transformación.

| Etapa | Herramientas | Justificación |
|---|---|---|
| Limpieza inicial | Python, Polars, PySpark | Útil para reglas simples, parsing de fechas y normalización |
| Transformación distribuida | Apache Spark, Databricks, AWS Glue | Adecuado para grandes volúmenes |
| Transformación SQL | dbt, Spark SQL, Trino, Snowflake SQL | Facilita modelos versionados y documentados |
| Calidad de datos | Great Expectations, dbt tests | Permite validar reglas y generar evidencia |
| Deduplicación e integración | Spark, dbt, SQL | Permite aplicar reglas de negocio y llaves homologadas |
| Features ML | PySpark, Python, Feature Store | Preparación de variables para ciencia de datos |
| Grafos | Neo4j, Amazon Neptune, TigerGraph | Modelado de relaciones y búsqueda en grafos |

---

## G. ¿Qué storage usarías para cada propósito y por qué?

### Storage para Data Lake

Usaría almacenamiento de objetos:

- Amazon S3.
- Azure Data Lake Storage.
- Google Cloud Storage.
- MinIO para desarrollo local.

Justificación:

- Escalable.
- Económico.
- Compatible con múltiples motores.
- Adecuado para datos históricos.
- Permite almacenar datos en distintas capas.

Formato recomendado:

- Parquet.
- Delta Lake.
- Apache Iceberg.
- Apache Hudi.

### Storage para consultas SQL operativas

Usaría un Data Warehouse:

- Snowflake.
- Amazon Redshift.
- BigQuery.
- Azure Synapse.

Justificación:

- Mejor rendimiento para consultas SQL.
- Compatible con BI.
- Permite controlar permisos por usuarios, roles y vistas.
- Facilita modelos dimensionales.

### Storage para ciencia de datos

Usaría:

- Data Lake en Parquet para históricos.
- Feature Store para variables reutilizables.
- Graph Database para relaciones complejas.

Herramientas:

- Databricks Feature Store.
- SageMaker Feature Store.
- Neo4j.
- Amazon Neptune.
- TigerGraph.

Justificación:

- Ciencia de datos requiere datos históricos, flexibles y con posibilidad de reprocesamiento.
- Los grafos requieren un motor especializado en relaciones, no solo tablas relacionales.

---