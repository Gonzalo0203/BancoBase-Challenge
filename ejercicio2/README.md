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