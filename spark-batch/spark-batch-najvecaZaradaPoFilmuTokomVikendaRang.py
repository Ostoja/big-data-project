#prikazuje najvece zarade filma tokom vikenda koji su bili na odredjenoj poziciji po zaradi


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

dfPom = df

dfPom.join(dfJoin, ((dfJoin["year"] == dfPom["year"]) & (dfJoin["calendarWeek"] == dfPom["calendarWeek"]) & (dfJoin["maxBoxOffice"] == dfPom["boxOffice"]))).select(dfPom["year"], dfPom["calendarWeek"], dfPom["boxOffice"], dfPom["movie_title"], dfJoin["sumBoxOffice"], dfJoin["percentage"], dfJoin["difference"], dfPom["genre"], dfPom["date"], dfPom["release_date"], dfPom["rank"]).orderBy("boxOffice", ascending =False).show(50)

df.join(dfPom, ((dfPom["year"] == df["year"]) & (dfPom["calendarWeek"] == df["calendarWeek"]))).select(df["year"], df["calendarWeek"], df["boxOffice"], df["movie_title"], df["genre"], df["date"], df["release_date"], df["rank"]).distinct().filter(f.col("rank").like("10")).orderBy("boxOffice", ascending =False).show(50)
