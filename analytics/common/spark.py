from pyspark.sql import SparkSession


def session(name, cfg):
    builder = SparkSession.builder.appName(name)
    options = {
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.shuffle.partitions": str(cfg["shuffle_partitions"]),
        "spark.databricks.delta.schema.autoMerge.enabled": "true",
        "spark.hadoop.fs.s3a.endpoint": cfg["minio_endpoint"],
        "spark.hadoop.fs.s3a.access.key": cfg["access_key"],
        "spark.hadoop.fs.s3a.secret.key": cfg["secret_key"],
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    }
    for key, value in options.items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
