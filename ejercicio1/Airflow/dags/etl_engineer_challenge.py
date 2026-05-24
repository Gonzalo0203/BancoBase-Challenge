from datetime import datetime
from io import BytesIO

import boto3
import polars as pl
import trino

from airflow.sdk import dag, task


# ============================================================
# Configuración general
# ============================================================

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio1234"

LANDING_BUCKET = "bck-landing"
BRONZE_BUCKET = "bck-bronze"

INPUT_KEY = "data/data_prueba_tecnica.csv"
OUTPUT_KEY = "master/data_prueba_tecnica.parquet"

TRINO_HOST = "trino"
TRINO_PORT = 8080
TRINO_USER = "admin_trino"

TRINO_CATALOG = "bronze"
TRINO_SCHEMA = "prueba"
TRINO_TABLE = "tbl_data"


# ============================================================
# Cliente de MinIO
# ============================================================

def get_minio_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )


# ============================================================
# Definición del DAG
# ============================================================

@dag(
    dag_id="etl_engineer_challenge",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # Se ejecuta manualmente
    catchup=False,
    tags=["challenge", "etl", "minio", "trino"],
)
def etl_engineer_challenge():

    @task
    def create_buckets():
        """
        Crea los buckets necesarios en MinIO.
        Si ya existen, no hace nada.
        """

        s3 = get_minio_client()

        existing_buckets = [
            bucket["Name"] for bucket in s3.list_buckets()["Buckets"]
        ]

        for bucket_name in [LANDING_BUCKET, BRONZE_BUCKET]:
            if bucket_name not in existing_buckets:
                s3.create_bucket(Bucket=bucket_name)
                print(f"Bucket creado: {bucket_name}")
            else:
                print(f"Bucket ya existe: {bucket_name}")

    @task
    def validate_input_file():
        """
        Valida que el archivo CSV exista en el bucket landing.
        """

        s3 = get_minio_client()

        try:
            s3.head_object(
                Bucket=LANDING_BUCKET,
                Key=INPUT_KEY,
            )
            print(f"Archivo encontrado: s3://{LANDING_BUCKET}/{INPUT_KEY}")

        except Exception as error:
            raise FileNotFoundError(
                f"No se encontró el archivo s3://{LANDING_BUCKET}/{INPUT_KEY}"
            ) from error

    @task
    def process_csv_to_parquet():
        """
        Lee el CSV desde MinIO, detecta inconsistencias básicas,
        corrige valores simples, genera agregaciones por name y created_at,
        y guarda el resultado en formato Parquet.
        """

        s3 = get_minio_client()

        # Leer archivo CSV desde MinIO
        response = s3.get_object(
            Bucket=LANDING_BUCKET,
            Key=INPUT_KEY,
        )

        csv_bytes = response["Body"].read()

        df = pl.read_csv(BytesIO(csv_bytes))

        # Limpiar nombres de columnas, por ejemplo paid_at\r -> paid_at
        df = df.rename({
            col: col.strip()
            for col in df.columns
        })

        print("Columnas encontradas:")
        print(df.columns)

        print(f"Total de registros de entrada: {df.height}")

        # ========================================================
        # 1. Detección básica de inconsistencias
        # ========================================================

        total_ids_null = df.select(
            pl.col("id").is_null().sum()
        ).item()

        total_names_null_or_empty = df.select(
            (
                pl.col("name").is_null()
                | (pl.col("name").cast(pl.Utf8).str.strip_chars() == "")
            ).sum()
        ).item()

        total_company_id_null_or_empty = df.select(
            (
                pl.col("company_id").is_null()
                | (pl.col("company_id").cast(pl.Utf8).str.strip_chars() == "")
            ).sum()
        ).item()

        print(f"Ids nulos detectados: {total_ids_null}")
        print(f"Nombres nulos o vacíos detectados: {total_names_null_or_empty}")
        print(f"Company_id nulos o vacíos detectados: {total_company_id_null_or_empty}")

        # ========================================================
        # 2. Limpieza / corrección básica
        # ========================================================

        df_clean = (
            df
            .with_columns(
                [
                    # Normalizar name:
                    # - convertir a texto
                    # - quitar espacios al inicio y final
                    # - convertir a mayúsculas
                    # - si queda vacío, convertir a null
                    pl.when(
                        pl.col("name").is_null()
                        | (pl.col("name").cast(pl.Utf8).str.strip_chars() == "")
                    )
                    .then(None)
                    .otherwise(
                        pl.col("name")
                        .cast(pl.Utf8)
                        .str.strip_chars()
                        .str.replace_all(r"\s+", " ")
                        .str.to_uppercase()
                    )
                    .alias("name_clean"),

                    # Normalizar company_id:
                    # - convertir a texto
                    # - quitar espacios
                    # - si queda vacío, convertir a null
                    pl.when(
                        pl.col("company_id").is_null()
                        | (pl.col("company_id").cast(pl.Utf8).str.strip_chars() == "")
                    )
                    .then(None)
                    .otherwise(
                        pl.col("company_id")
                        .cast(pl.Utf8)
                        .str.strip_chars()
                    )
                    .alias("company_id_clean"),

                    # Convertir created_at a datetime.
                    # Si la fecha no se puede convertir, queda como null.
                    pl.col("created_at")
                    .cast(pl.Utf8)
                    .str.strptime(pl.Datetime, strict=False)
                    .alias("created_at_datetime"),
                ]
            )
            .with_columns(
                [
                    # Extraer solo la fecha para agregar por día
                    pl.col("created_at_datetime")
                    .dt.date()
                    .alias("created_date"),

                    # Flags simples para evidenciar detección de inconsistencias
                    pl.col("id")
                    .is_null()
                    .alias("id_is_null"),

                    pl.col("name_clean")
                    .is_null()
                    .alias("name_is_invalid"),

                    pl.col("company_id_clean")
                    .is_null()
                    .alias("company_id_is_invalid"),

                    pl.col("created_at_datetime")
                    .is_null()
                    .alias("created_at_is_invalid"),
                ]
            )
        )

        # Detectar fechas inválidas después de intentar convertir created_at
        total_created_at_invalid = df_clean.select(
            pl.col("created_at_is_invalid").sum()
        ).item()

        print(f"Fechas inválidas detectadas en created_at: {total_created_at_invalid}")

        # ========================================================
        # 3. Agregaciones sobre name y created_at
        # ========================================================
        # Se usa name_clean y created_date porque ya son campos limpios.

        df_result = (
            df_clean
            .group_by(["name_clean", "created_date"])
            .agg(
                [
                    pl.len().cast(pl.Int64).alias("total_records"),

                    pl.col("id_is_null")
                    .sum()
                    .cast(pl.Int64)
                    .alias("ids_nulls"),

                    pl.col("company_id_clean")
                    .drop_nulls()
                    .n_unique()
                    .cast(pl.Int64)
                    .alias("companies_distinct"),

                    pl.col("company_id_is_invalid")
                    .sum()
                    .cast(pl.Int64)
                    .alias("company_id_invalids"),

                    pl.col("created_at_is_invalid")
                    .sum()
                    .cast(pl.Int64)
                    .alias("created_at_invalids"),
                ]
            )
            .rename(
                {
                    "name_clean": "name",
                }
            )
            .sort(["created_date", "name"])
        )

        total_records_after_group = df_result.select(
            pl.col("total_records").sum()
        ).item()

        print(f"Total registros antes de agrupar: {df.height}")
        print(f"Total registros después de agrupar: {total_records_after_group}")
        print(df_result.head())

        if total_records_after_group != df.height:
            raise ValueError(
                "La suma de total_records no coincide con los registros originales"
            )

        # ========================================================
        # 4. Guardar resultado como Parquet
        # ========================================================

        parquet_buffer = BytesIO()
        df_result.write_parquet(parquet_buffer)

        parquet_buffer.seek(0)

        s3.put_object(
            Bucket=BRONZE_BUCKET,
            Key=OUTPUT_KEY,
            Body=parquet_buffer.getvalue(),
        )

        print(f"Archivo Parquet guardado en: s3://{BRONZE_BUCKET}/{OUTPUT_KEY}")

    @task
    def create_trino_schema_and_table():
        """
        Crea el schema y la tabla externa en Trino
        apuntando al archivo Parquet guardado en MinIO.
        """

        conn = trino.dbapi.connect(
            host=TRINO_HOST,
            port=TRINO_PORT,
            user=TRINO_USER,
            catalog=TRINO_CATALOG,
        )

        cursor = conn.cursor()

        # Crear schema dentro del catálogo bronze
        cursor.execute(f"""
            CREATE SCHEMA IF NOT EXISTS {TRINO_CATALOG}.{TRINO_SCHEMA}
            WITH (
                location = 's3a://{BRONZE_BUCKET}/'
            )
        """)

        print(f"Schema creado o existente: {TRINO_CATALOG}.{TRINO_SCHEMA}")

        # Para simplificar, eliminamos la tabla si ya existe
        cursor.execute(f"""
            DROP TABLE IF EXISTS {TRINO_CATALOG}.{TRINO_SCHEMA}.{TRINO_TABLE}
        """)

        # Crear tabla externa sobre la carpeta donde está el Parquet
        cursor.execute(f"""
            CREATE TABLE {TRINO_CATALOG}.{TRINO_SCHEMA}.{TRINO_TABLE} (
                name VARCHAR,
                created_date DATE,
                total_records BIGINT,
                ids_nulls BIGINT,
                companies_distinct BIGINT
            )
            WITH (
                external_location = 's3a://{BRONZE_BUCKET}/master/',
                format = 'PARQUET'
            )
        """)

        print(
            f"Tabla creada: "
            f"{TRINO_CATALOG}.{TRINO_SCHEMA}.{TRINO_TABLE}"
        )

    @task
    def validate_trino_table():
        """
        Ejecuta una consulta sencilla para validar que Trino
        pueda leer la tabla creada.
        """

        conn = trino.dbapi.connect(
            host=TRINO_HOST,
            port=TRINO_PORT,
            user=TRINO_USER,
            catalog=TRINO_CATALOG,
            schema=TRINO_SCHEMA,
        )

        cursor = conn.cursor()

        cursor.execute(f"""
            SELECT COUNT(*) 
            FROM {TRINO_CATALOG}.{TRINO_SCHEMA}.{TRINO_TABLE}
        """)

        result = cursor.fetchone()

        print(f"Total de registros consultados desde Trino: {result[0]}")

    # ========================================================
    # Orden de ejecución de las tareas
    # ========================================================

    (
        create_buckets()
        >> validate_input_file()
        >> process_csv_to_parquet()
        >> create_trino_schema_and_table()
        >> validate_trino_table()
    )


etl_engineer_challenge()