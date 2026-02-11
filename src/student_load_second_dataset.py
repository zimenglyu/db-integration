from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from config import Settings, dataset_path, load_settings


def append_second_dataset(spark: SparkSession, settings: Settings) -> DataFrame:
    """TODO(student): read dataset 2 and append it into PostgreSQL."""
    # Suggested implementation outline:
    # 1) Read CSV2 from dataset_path(settings, settings.second_dataset_file)
    # 2) Write DataFrame to settings.pg_table in append mode via JDBC
    # 3) Return the DataFrame that was appended
    raise NotImplementedError("Implement the second dataset load in this function.")


def main() -> None:
    settings = load_settings()
    spark = (
        SparkSession.builder.appName("CPS5721StudentLoad")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .getOrCreate()
    )

    before = (
        spark.read.format("jdbc")
        .option("url", settings.jdbc_url)
        .option("dbtable", settings.pg_table)
        .option("user", settings.pg_user)
        .option("password", settings.pg_password)
        .option("driver", "org.postgresql.Driver")
        .load()
        .count()
    )

    # Useful input path for the TODO implementation.
    csv2_path = dataset_path(settings, settings.second_dataset_file)
    print(f"Second dataset path: {csv2_path}")

    append_second_dataset(spark, settings)

    after = (
        spark.read.format("jdbc")
        .option("url", settings.jdbc_url)
        .option("dbtable", settings.pg_table)
        .option("user", settings.pg_user)
        .option("password", settings.pg_password)
        .option("driver", "org.postgresql.Driver")
        .load()
        .count()
    )

    print(f"Rows before load: {before}")
    print(f"Rows after load:  {after}")
    print(f"Rows inserted:    {after - before}")
    spark.stop()


if __name__ == "__main__":
    main()
