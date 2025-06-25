from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max as max_, row_number
from pyspark.sql.window import Window
from flask import Flask, jsonify

# Initialize Spark
spark = SparkSession.builder \
    .appName("PySparkProject") \
    .getOrCreate()

# Load CSV and infer schema
df = spark.read.csv("data/sample.csv", header=True, inferSchema=True)

# Register temp view for SQL
df.createOrReplaceTempView("employees")

# Loader function (can add more later)
def load_data():
    return spark.sql("SELECT * FROM employees")

# Grouping & Aggregation
grouped = df.groupBy("department").agg(
    avg("salary").alias("avg_salary"),
    max_("salary").alias("max_salary")
)

# Window function: rank highest salary per department
window_spec = Window.partitionBy("department").orderBy(col("salary").desc())
ranked = df.withColumn("rank", row_number().over(window_spec))

# Basic API using Flask
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
