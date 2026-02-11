from __future__ import annotations

from pyspark.sql import SparkSession

from config import dataset_path, load_settings


def main() -> None:
    settings = load_settings()
    spark = SparkSession.builder.appName("CPS5721LoadFirstDataset").getOrCreate()

    csv_path = dataset_path(settings, settings.first_dataset_file)
    df = spark.read.option("header", True).option("inferSchema", True).csv(csv_path)

    (
        df.write.format("jdbc")
        .option("url", settings.jdbc_url)
        .option("dbtable", settings.pg_table)
        .option("user", settings.pg_user)
        .option("password", settings.pg_password)
        .option("driver", "org.postgresql.Driver")
        .mode("overwrite")
        .save()
    )

    print(f"Loaded first dataset rows: {df.count()}")
    spark.stop()


if __name__ == "__main__":
    main()
