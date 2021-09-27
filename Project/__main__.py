from pyspark import SparkContext
from pyspark.sql import SparkSession

sc = SparkContext("local[*]", "my-first-application").getOrCreate()
ss = SparkSession(sc)

df_raw = ss.read.format("csv").load("/home/ubuntu/data")
df_raw.createGlobalTempView("input_data")
df_aggregated = ss.sql("SELECT mean(_c1) FROM global_temp.input_data GROUP BY _c0, _c1")
df_aggregated.write.csv("/home/ubuntu/data_aggregated.csv")