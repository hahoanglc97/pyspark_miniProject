from pyspark import SparkConf, SparkContext
conf = SparkConf{).setAppName{'Read File').set{"spark.ui.port", "8080")
sc = SparkContext.getOrCreate{conf=conf)
text = sc.textFile{'sample.txt')
print{text.collect{))