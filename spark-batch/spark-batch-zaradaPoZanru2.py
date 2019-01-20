#prikazuje maksimalnu zaradu po zanru u nedelji i film sa najvecom zaradom u toj nedelji tog zanra.


from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as f

def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

conf = SparkConf().setAppName("bmojo").setMaster("local")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

quiet_logs(spark)

from pyspark.sql.types import *

schemaString = "movie_id movie_title domestic_total_gross release_date close_date in_release_days runtime_mins rating genre distributor director producer production_budget widest_release_theaters actors writers cinematographers composers year calendarWeek date rank boxOffice theatres grossBoxOffice longWeekend"
fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
schema = StructType(fields)

df = spark.read.format("csv").option("delimiter", "\t").schema(schema).load("hdfs://namenode:8020/user/ostoja/bmojo/bmojoreduced.tsv")

df = df.withColumn("boxOffice", df["boxOffice"].cast(IntegerType()))

df = df.withColumn("grossBoxOffice", df["grossBoxOffice"].cast(IntegerType()))
##print df.groupBy("year", "calendarWeek").sum("boxOffice").orderBy("sum(boxOffice)", ascending = False).show(50) #prikazimo zaradu po nedeljama

##print df.groupBy("movie_id").sum("boxOffice").orderBy("sum(boxOffice)", ascending = False).show(50) #filmovi po zaradama

##print df.groupBy("year", "calendarWeek", "genre").sum("boxOffice").orderBy("year", "calendarWeek", "sum(boxOffice)", ascending = False).filter(f.col("calendarWeek").like("17")).orderBy("sum(boxOffice)", ascending = False).show(10000)

#print df.groupBy("year", "calendarWeek", "genre").agg(f.count(f.lit(1)).alias("cntMovies"), f.sum("boxOffice").alias("sumBoxOffice")).orderBy("year", "calendarWeek", "sumBoxOffice", ascending = False).filter(f.col("calendarWeek").like("17")).show(20)

print df.groupBy("year", "calendarWeek", "genre").agg(f.count(f.lit(1)).alias("cntMovies"), f.sum("boxOffice").alias("sumBoxOffice"), f.max("boxOffice").alias("maxBoxOffice")).orderBy("year", "calendarWeek", "sumBoxOffice", ascending = False).filter(f.col("calendarWeek").like("21")).orderBy("sumboxOffice", ascending = False).show(20)
