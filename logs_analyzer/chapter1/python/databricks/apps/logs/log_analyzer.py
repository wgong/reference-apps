"""
To run this code:

$ spark-submit \
    --master local[4] \
    --py-files databricks/apps/logs/apache_access_log.py \
    databricks/apps/logs/log_analyzer.py \
    ../../data/apache.accesslog
## not working

$ python log_analyzer.py /home/wengong/spark/apps/databrick-ref-apps/logs_analyzer/data/apache.access.log
"""

from pyspark import SparkContext, SparkConf

# import apache_access_log
from apache_access_log import parse_apache_log_line
import sys

conf = SparkConf().setAppName("Log Analyzer").setMaster("local[*]")
sc = SparkContext(conf=conf)

logFile = sys.argv[1]

access_logs = (sc.textFile(logFile)
               .map(parse_apache_log_line)
               .cache())

num_lines = access_logs.count()  # trigger cache

# Calculate statistics based on the content size.
content_sizes = access_logs.map(lambda log: log.content_size).cache()
print("\nContent Size Stat:\nAvg: %.2f, Min: %i, Max: %s" % (
    content_sizes.reduce(lambda a, b : a + b) / num_lines,
    content_sizes.min(),
    content_sizes.max()
    ))

# Response Code to Count
responseCodeToCount = (access_logs.map(lambda log: (log.response_code, 1))
                       .reduceByKey(lambda a, b : a + b)
                       .take(100))
print("\nResponse Code Counts:\n %s" % (responseCodeToCount))

# Any IPAddress that has accessed the server more than 10 times.
ipAddresses = (access_logs
               .map(lambda log: (log.ip_address, 1))
               .reduceByKey(lambda a, b : a + b)
               .filter(lambda s: s[1] > 10)
               .map(lambda s: s[0])
               .take(10))
print("\nIpAddresses that have accessed more then 10 times:\n%s" % ("\n".join(ipAddresses)))

# Top Endpoints
topEndpoints = (access_logs
                .map(lambda log: (log.endpoint, 1))
                .reduceByKey(lambda a, b : a + b)
                .takeOrdered(10, lambda s: -1 * s[1]))

topEndpoints = [f"{i[0]}: {i[1]}" for i in topEndpoints]
print("\nTop Endpoints:\n%s" % ("\n".join(topEndpoints)))
