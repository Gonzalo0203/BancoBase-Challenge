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