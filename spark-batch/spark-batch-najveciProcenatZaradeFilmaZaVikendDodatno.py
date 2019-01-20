#prikazuje najveci procenat zarade filma u odnosu na ukupnu zaradu svih filmova za taj vikend, prikazuje i dodatne informacije


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


dfJoin = df.groupBy("year", "calendarWeek").agg(f.sum("boxOffice").alias("sumBoxOffice"), f.max("boxOffice").alias("maxBoxOffice"), (f.max("boxOffice") / f.sum("boxOffice")).alias("percentage"), (f.sum("boxOffice") - f.max("boxOffice")).alias("difference")).orderBy("percentage", ascending = False)#.show(50)

df.join(dfJoin, ((dfJoin["year"] == df["year"]) & (dfJoin["calendarWeek"] == df["calendarWeek"]) & (dfJoin["maxBoxOffice"] == df["boxOffice"]))).select(df["year"], df["calendarWeek"], df["boxOffice"], df["movie_title"], dfJoin["sumBoxOffice"], dfJoin["percentage"], dfJoin["difference"], df["genre"], df["date"], df["release_date"]).orderBy("boxOffice", ascending =False).show(50)

