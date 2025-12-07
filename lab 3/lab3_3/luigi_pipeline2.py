import luigi
import os
import subprocess
import time
import csv
import pandas as pd
from pyhive import hive
import glob
import shutil

# --- ПУТИ И НАСТРОЙКИ ---
OUTPUT_DIR = "data_mart_output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Hive
HIVE_DOCKER_DIR = r"E:\Python_LabsAndProjekt\mathmod\Veresk\task 2\punkt 2\docker-hive"
HIVE_HOST = "127.0.0.1"
HIVE_PORT = 10000
HIVE_DB = "switrs"
HIVE_TABLE = "collisions_small"
HIVE_CSV_PATH = "/tmp/collisions_small.csv"
HIVE_OUTPUT = "hive_top_10_counties.csv"

# Spark
SPARK_DOCKER_DIR = r"E:\Python_LabsAndProjekt\mathmod\Veresk\task 2\punkt 3"
SPARK_MASTER = "spark-master"
SPARK_SCRIPT = "/opt/spark/work-dir/spark_switrs.py"
SPARK_OUTPUT_CSV = "spark_density_metric.csv"
SPARK_CONTAINER_OUTPUT_DIR = "/opt/spark/work-dir/spark_output_temp"

# Final Datamart
FINAL_MART = "final_datamart.csv"

class StartHive(luigi.Task):
    def output(self):
        return luigi.LocalTarget(os.path.join(OUTPUT_DIR, "hive_ok.txt"))

    def run(self):
        subprocess.run(["docker-compose", "up", "-d"],
                       cwd=HIVE_DOCKER_DIR,
                       check=True)
        time.sleep(10)
        open(self.output().path, "w").write("ok")


class HiveAnalysis(luigi.Task):
    def requires(self):
        return StartHive()

    def output(self):
        return luigi.LocalTarget(os.path.join(OUTPUT_DIR, HIVE_OUTPUT))

    def run(self):
        conn = hive.Connection(host=HIVE_HOST, port=HIVE_PORT, username="hive")
        cur = conn.cursor()

        cur.execute(f"CREATE DATABASE IF NOT EXISTS {HIVE_DB}")
        cur.execute(f"USE {HIVE_DB}")

        cur.execute(f"DROP TABLE IF EXISTS {HIVE_TABLE}")
        cur.execute(
            f"""
            CREATE TABLE {HIVE_TABLE} (
                county_location STRING,
                county_city_location STRING
            )
            ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE
            """
        )

        cur.execute(f"LOAD DATA LOCAL INPATH '{HIVE_CSV_PATH}' OVERWRITE INTO TABLE {HIVE_TABLE}")

        cur.execute(
            f"""
            SELECT county_location, COUNT(*) AS total_accidents
            FROM {HIVE_TABLE}
            GROUP BY county_location
            ORDER BY total_accidents DESC
            LIMIT 10
            """
        )
        rows = cur.fetchall()

        with open(self.output().path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["county_location", "total_accidents"])
            writer.writerows(rows)

        cur.close()
        conn.close()


class SparkAnalysis(luigi.Task):
    def output(self):
        return luigi.LocalTarget(os.path.join(OUTPUT_DIR, SPARK_OUTPUT_CSV))

    def requires(self):
        return StartHive()  # Spark и Hive запускаем одновременно

    def run(self):
        subprocess.run(["docker-compose", "up", "-d"],
                       cwd=SPARK_DOCKER_DIR,
                       check=True)
        time.sleep(5)

        # Запуск spark-submit в контейнере
        subprocess.run([
            "docker", "exec", SPARK_MASTER,
            "/opt/spark/bin/spark-submit",
            "--master", "spark://spark-master:7077",
            SPARK_SCRIPT,
            SPARK_CONTAINER_OUTPUT_DIR
        ], check=True)

        # Копируем директорию spark_output_temp
        local_temp = os.path.join(OUTPUT_DIR, "spark_output_temp")
        if os.path.exists(local_temp):
            shutil.rmtree(local_temp)

        subprocess.run([
            "docker", "cp",
            f"{SPARK_MASTER}:{SPARK_CONTAINER_OUTPUT_DIR}",
            OUTPUT_DIR
        ], check=True)

        # Ищем файл part-00000-*.csv
        part_file = glob.glob(os.path.join(local_temp, "part-*.csv"))[0]

        shutil.move(part_file, self.output().path)
        shutil.rmtree(local_temp)

class FinalDataMart(luigi.Task):

    def requires(self):
        return [HiveAnalysis(), SparkAnalysis()]

    def output(self):
        return luigi.LocalTarget(os.path.join(OUTPUT_DIR, FINAL_MART))

    def run(self):
        hive_df = pd.read_csv(self.input()[0].path)
        spark_df = pd.read_csv(self.input()[1].path)

        df = hive_df.merge(spark_df, on="county_location", how="inner")
        df.to_csv(self.output().path, index=False)

if __name__ == "__main__":
    luigi.run()
