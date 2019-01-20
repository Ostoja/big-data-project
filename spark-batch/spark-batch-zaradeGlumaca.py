#prikazuje zarade filmova dok su bili 1. na vikend listi po glumcu

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as f



def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)


conf = SparkConf().setAppName("imdb bmojo join").setMaster("local")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

quiet_logs(spark)

schemaString = "movie_id movie_title domestic_total_gross release_date close_date in_release_days runtime_mins rating genre distributor director producer production_budget widest_release_theaters actors writers cinematographers composers year calendarWeek date rank boxOffice theatres grossBoxOffice longWeekend"
fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
schema = StructType(fields)

df = spark.read.format("csv").option("delimiter", "\t").schema(schema).load("hdfs://namenode:8020/user/ostoja/bmojo/bmojoreduced.tsv")

df = df.withColumn("release_date", f.split('release_date', '-')[0])
df = df.withColumn("boxOffice", df["boxOffice"].cast(IntegerType()))

df = df.withColumn("grossBoxOffice", df["grossBoxOffice"].cast(IntegerType()))

print df.groupBy("year", "calendarWeek").agg(f.count(f.lit(1)).alias("cntMovies")).orderBy("cntMovies", ascending = True).show(50)

schemaStringIMDB = "tconst titleType primaryTitle originalTitle	isAdult	startYear endYear runtimeMinutes genres"
fieldsIMDB = [StructField(field_name, StringType(), True) for field_name in schemaStringIMDB.split()]
schemaIMDB = StructType(fieldsIMDB)

dfIMDB = spark.read.format("csv").option("delimiter", "\t").schema(schemaIMDB).load("hdfs://namenode:8020/user/ostoja/bmojo/movie2000.tsv")

schemaStringIMDBPrincipals = "tconst ordering nconst category job characters"
fieldsIMDBPrincipals = [StructField(field_name, StringType(), True) for field_name in schemaStringIMDBPrincipals.split()]
schemaIMDBPrincipals = StructType(fieldsIMDBPrincipals)

dfIMDBPrincipals = spark.read.format("csv").option("delimiter", "\t").schema(schemaIMDBPrincipals).load("hdfs://namenode:8020/imdbprincipals")

schemaStringIMDBPeople = "nameconst primaryName	birthYear deathYear primaryProfession knownForTitles"
fieldsIMDBPeople = [StructField(field_name, StringType(), True) for field_name in schemaStringIMDBPeople.split()]
schemaIMDBPeople = StructType(fieldsIMDBPeople)

dfIMDBPeople = spark.read.format("csv").option("delimiter", "\t").schema(schemaIMDBPeople).load("hdfs://namenode:8020/imdbpeople")

print dfIMDBPeople.select("nameconst", "primaryName").show(50)

df = df.join(dfIMDB, ((df["movie_title"]==dfIMDB["originalTitle"]) & (df["release_date"]==dfIMDB["startYear"])))
df = df.distinct()
df = df.join(dfIMDBPrincipals, ((df["tconst"]==dfIMDBPrincipals["tconst"])))
df = df.distinct()
df = df.join(dfIMDBPeople, ((df["nconst"]==dfIMDBPeople["nameconst"])))
df = df.distinct()

print df.filter(f.col("category").like("actor") | f.col("category").like("actress")).filter(f.col("startYear").like("20%")).filter(f.col("rank").like("1")).select("boxOffice", "movie_id", "startYear", "primaryName", "primaryTitle", "nameconst").distinct().groupBy("nameconst", "primaryName").agg(f.sum("boxOffice").alias("sumBoxOffice")).orderBy("sumBoxOffice", ascending = False).show(100)

