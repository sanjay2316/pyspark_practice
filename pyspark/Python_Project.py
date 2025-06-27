from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max as max_, row_number
from pyspark.sql.window import Window
from flask import Flask, jsonify

spark = SparkSession.builder \
    .appName("PySparkProject") \
    .getOrCreate()


df = spark.read.csv("data/sample.csv", header=True, inferSchema=True)


df.createOrReplaceTempView("employees")


def load_data():
    return spark.sql("SELECT * FROM employees")


grouped = df.groupBy("department").agg(
    avg("salary").alias("avg_salary"),
    max_("salary").alias("max_salary")
)


window_spec = Window.partitionBy("department").orderBy(col("salary").desc())
ranked = df.withColumn("rank", row_number().over(window_spec))


app = Flask(__name__)

@app.route('/all', methods=['GET'])
def get_all():
    data = load_data().toJSON().collect()
    return jsonify([eval(row) for row in data])

@app.route('/grouped', methods=['GET'])
def get_grouped():
    data = grouped.toJSON().collect()
    return jsonify([eval(row) for row in data])

@app.route('/top', methods=['GET'])
def get_top_salaries():
    top_salaries = ranked.filter(col("rank") == 1)
    data = top_salaries.toJSON().collect()
    return jsonify([eval(row) for row in data])

if __name__ == '__main__':
    app.run(port=5000)
