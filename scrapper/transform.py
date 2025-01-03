"""
This script head .xlsx files and parse each file into a folder with its respective marker as a file.

    e.g
    ...
    2023/
    |___ Ameaça.csv
    |___ Lesão Corporal.csv
    ...

    2024/
    |___ Ameaça.csv
    |___ Lesão Corporal.csv

Author: João Augusto Tonial Laner
Github: https://github.com/joaoaugustolaner/
"""

__version__ = "0.1.0"

from pathlib import Path
import pandas as pd

from pyspark import SparkConf
from pyspark.sql import SparkSession


def processData(path_to_file: Path):
    # Spark info
    conf = (
        SparkConf()
        .setAppName("MapFemRS")
        .setMaster("local")
        .set("spark.executor.memory", "2g")
    )
    spark = SparkSession.Builder().config(conf=conf).getOrCreate()

    files = list(path_to_file.iterdir())
    sheet_names = [
        "Feminicídio Tentado",
        "Feminicídio Consumado",
        "Ameaça",
        "Estupro",
        "Lesão Corporal",
    ]

    for file in files:
        if file.stem == "2017":
            continue

        print(f"{file.stem} being saved...\n")

        output_path = Path(f"{Path.cwd().parent}/api/data/{file.stem}/")
        output_path.mkdir(exist_ok=True, parents=True)

        for sheet in sheet_names:
            df = pd.read_excel(
                file, engine="openpyxl", sheet_name=sheet, skiprows=3, nrows=497
            )

            df = spark.createDataFrame(df)
            df = df.drop("Unnamed: 0")

            df = (
                df.withColumnRenamed(f"{df.columns[0]}", "Cidades")
                .withColumnRenamed(f"{df.columns[1]}", "JAN")
                .withColumnRenamed(f"{df.columns[2]}", "FEV")
                .withColumnRenamed(f"{df.columns[3]}", "MAR")
                .withColumnRenamed(f"{df.columns[4]}", "ABR")
                .withColumnRenamed(f"{df.columns[5]}", "MAI")
                .withColumnRenamed(f"{df.columns[6]}", "JUN")
                .withColumnRenamed(f"{df.columns[7]}", "JUL")
                .withColumnRenamed(f"{df.columns[8]}", "AGO")
                .withColumnRenamed(f"{df.columns[9]}", "SEP")
                .withColumnRenamed(f"{df.columns[10]}", "OUT")
                .withColumnRenamed(f"{df.columns[11]}", "NOV")
                .withColumnRenamed(f"{df.columns[12]}", "DEZ")
                .withColumnRenamed(f"{df.columns[13]}", "TOTAL")
            )

            df = df.drop(df.columns[13])
            df.toPandas().to_csv(
                f"{output_path}/{sheet}.csv", encoding="utf-8", index=False, header=True
            )


if __name__ == "__main__":
    processData(Path(f"{Path.cwd()}/raw"))
