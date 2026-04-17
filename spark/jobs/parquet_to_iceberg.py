from pyspark.sql import SparkSession

NESSIE_URI = "http://nessie-catalog:19120/api/v2"
S3_ENDPOINT = "http://minio:9000"

WAREHOUSE = "s3://lakehouse"

AWS_ACCESS_KEY = "minioadmin"
AWS_SECRET_KEY = "minioadmin"
AWS_REGION = "us-east-1"


spark = (
    SparkSession.builder.appName("MinIO_Nessie_Iceberg")
    # ---------------- Iceberg + Nessie ----------------
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
        "org.projectnessie.spark.extensions.NessieSparkSessionExtensions",
    )
    .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
    .config(
        "spark.sql.catalog.nessie.catalog-impl",
        "org.apache.iceberg.nessie.NessieCatalog",
    )
    .config("spark.sql.catalog.nessie.uri", NESSIE_URI)
    .config("spark.sql.catalog.nessie.ref", "main")
    .config("spark.sql.catalog.nessie.warehouse", WAREHOUSE)
    .config("spark.sql.catalog.nessie.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.defaultCatalog", "nessie")
    # ---------------- S3A (MinIO) ----------------
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.endpoint.region", AWS_REGION)
    # ---------------- JARS (IMPORTANT FIX) ----------------
    .config(
        "spark.jars.packages",
        ",".join(
            [
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.13:1.6.1",
                "org.projectnessie:nessie-spark-extensions-3.5_2.13:0.95.0",
                "org.apache.hadoop:hadoop-aws:3.3.2",
                "com.amazonaws:aws-java-sdk-bundle:1.12.115",
            ]
        ),
    )
    .getOrCreate()
)

print("=====================================")
print("==> Spark Started")
print("=====================================")

# -----------------------------
# READ FROM MINIO (Parquet)
# -----------------------------
RAW_PATH = "s3a://lakehouse/raw/"

TABLES = {
    "olist_customers.parquet": "customers",
    "olist_geolocation.parquet": "geolocation",
    "olist_orders.parquet": "orders",
    "olist_order_items.parquet": "order_items",
    "olist_order_payments.parquet": "order_payments",
    "olist_order_reviews.parquet": "order_reviews",
    "olist_products.parquet": "products",
    "olist_sellers.parquet": "sellers",
    "product_category_name_translation.parquet": "product_category_name_translation",
}

spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.default")

for file_name, table_name in TABLES.items():
    path = RAW_PATH + file_name

    print(f"Processing {path} → nessie.default.{table_name}")

    df = spark.read.parquet(path)
    df.show(5)

    df.writeTo(f"nessie.default.{table_name}").createOrReplace()

print("=====================================")
print("All tables ingested")
print("=====================================")

spark.stop()
