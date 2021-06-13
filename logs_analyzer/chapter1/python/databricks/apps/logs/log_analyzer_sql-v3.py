#!/usr/bin/env python
# coding: utf-8


from pyspark.sql import SparkSession
import apache_access_log


spark = SparkSession.builder.appName("Log Analyzer").getOrCreate()
sc = spark.sparkContext

logFile = "/home/wengong/spark/apps/databrick-ref-apps/logs_analyzer/data/apache.access.log"

access_logs = sc.textFile(logFile).map(apache_access_log.parse_apache_log_line)


type(access_logs)

row1 = access_logs.first()

type(row1)

columns = list(row1.asDict().keys())

columns

df = access_logs.toDF(columns)

df = df.drop("client_identd", "user_id")

df.show(5)

df.createOrReplaceTempView("logs")

# Calculate statistics based on the content size.

sql = """
SELECT 
    SUM(content_size) as theSum,
    COUNT(*) as theCount,
    Avg(content_size) as theAvg,
    MIN(content_size) as theMin,
    MAX(content_size) as theMax
FROM logs
"""
spark.sql(sql).show()

# Response Code to Count

sql = """
SELECT response_code, COUNT(*) AS theCount 
FROM logs 
GROUP BY response_code 
order by COUNT(*) desc
"""
spark.sql(sql).show()

# Any IPAddress that has accessed the server more than 10 times


sql = """
SELECT ip_address, COUNT(*) AS theCount 
FROM logs 
GROUP BY ip_address 
order by COUNT(*) desc
"""
spark.sql(sql).show(10, truncate=False)

# Top Endpoints

sql = """
SELECT endpoint, COUNT(*) AS theCount 
FROM logs 
GROUP BY endpoint 
order by COUNT(*) desc
"""
spark.sql(sql).show(10, truncate=False)

spark.stop()

