// Databricks notebook source
// MAGIC %md
// MAGIC ## Jak działa partycjonowanie
// MAGIC 
// MAGIC 1. Rozpocznij z 8 partycjami.
// MAGIC 2. Uruchom kod.
// MAGIC 3. Otwórz **Spark UI**
// MAGIC 4. Sprawdź drugi job (czy są jakieś różnice pomięczy drugim)
// MAGIC 5. Sprawdź **Event Timeline**
// MAGIC 6. Sprawdzaj czas wykonania.
// MAGIC   * Uruchom kilka razy rzeby sprawdzić średni czas wykonania.
// MAGIC 
// MAGIC Powtórz z inną liczbą partycji
// MAGIC * 1 partycja
// MAGIC * 7 partycja
// MAGIC * 9 partycja
// MAGIC * 16 partycja
// MAGIC * 24 partycja
// MAGIC * 96 partycja
// MAGIC * 200 partycja
// MAGIC * 4000 partycja
// MAGIC 
// MAGIC Zastąp `repartition(n)` z `coalesce(n)` używając:
// MAGIC * 6 partycji
// MAGIC * 5 partycji
// MAGIC * 4 partycji
// MAGIC * 3 partycji
// MAGIC * 2 partycji
// MAGIC * 1 partycji
// MAGIC 
// MAGIC ** *Note:* ** *Dane muszą być wystarczająco duże żeby zaobserwować duże różnice z małymi partycjami.*<br/>* To co możesz sprawdzić jak zachowują się małe dane z dużą ilośćia partycji.*

// COMMAND ----------

// val slots = sc.defaultParallelism
spark.conf.get("spark.sql.shuffle.partitions")

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val schema = StructType(
  List(
    StructField("timestamp", StringType, false),
    StructField("site", StringType, false),
    StructField("requests", IntegerType, false)
  )
)

val fileName = "dbfs:/FileStore/tables/pageviews_by_second.tsv"

val initialDF = spark.read
  .option("header", "true")
  .option("sep", "\t")
  .schema(schema)
  .csv(fileName)

// COMMAND ----------

spark.catalog.clearCache()
val parquetDir = "dbfs:/FileStore/tables/pageviews_by_second.parquet"

// COMMAND ----------

 val df = spark.read.parquet(parquetDir).repartition(8)

// COMMAND ----------

df.rdd.getNumPartitions

// COMMAND ----------

df.explain

// COMMAND ----------

df.count()

// COMMAND ----------

df.repartition(1)
df.count()

// COMMAND ----------

df.repartition(7)
df.count()

// COMMAND ----------

df.repartition(9)
df.count()

// COMMAND ----------

df.repartition(16)
df.count()

// COMMAND ----------

df.repartition(24)
df.count()

// COMMAND ----------

df.repartition(96)
df.count()

// COMMAND ----------

df.repartition(200)
df.count()

// COMMAND ----------

df.repartition(4000)
df.count()

// COMMAND ----------

df.coalesce(6)
df.count

// COMMAND ----------

df.coalesce(5)
df.count

// COMMAND ----------

df.coalesce(4)
df.count

// COMMAND ----------

df.coalesce(3)
df.count

// COMMAND ----------

df.coalesce(2)
df.count

// COMMAND ----------

df.coalesce(1)
df.count

// COMMAND ----------

  df.explain

// COMMAND ----------

df.rdd.getNumPartitions

// COMMAND ----------

// MAGIC %md
// MAGIC Zadanie 2
// MAGIC 
// MAGIC Poka Yoke
// MAGIC 
// MAGIC Napisz 6 metod, które mogą być użyte w Pipeline tak aby były odporne na błędy użytkownika, jak najbardziej „produkcyjnie”. Możesz użyć tego co już stworzyłeś i usprawnij rozwiązanie na bardziej odporne na błędy biorąc pod uwagę dobre praktyki.

// COMMAND ----------

import org.apache.spark.sql.types._

val ActorsSchema = StructType(Array(
    StructField("imdb_title_id",StringType,true),
    StructField("ordering",IntegerType,true),
    StructField("imdb_name_id",StringType,true),
    StructField("category",StringType,true),
    StructField("job",StringType,true),
    StructField("characters",StringType,true)))
val filePath = "dbfs:/FileStore/tables/Files/actors.csv"
val file=spark.read.format("csv") 
            .option("header","true") 
            .schema(ActorsSchema) 
            .load(filePath)
file.show

// COMMAND ----------

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.log4j.{Level, Logger}
import org.scalatest.Assertions._

// COMMAND ----------

def Count_Null_in_Column(colName : String, df :DataFrame) : Either[String,Long] ={
  if(df.columns.contains(colName)){
     val count =df.filter(col(colName).isNull).count()
     Right(count)
  }else{
    Left("No such column in data")
  }
}

// COMMAND ----------

def Count_Mean_in_Column(colName : String, df :DataFrame) ={
  if(df.columns.contains(colName)&df.schema(colName).dataType.typeName == "integer"){
    df.select(mean(df(colName))).show()
    
  }else{
    if(df.schema(colName).dataType.typeName != "integer"){
      Logger.getLogger("column type is not integer").setLevel(Level.ERROR)
    }else{
      Logger.getLogger("Such a column does not exist in df").setLevel(Level.ERROR)
    }
  }
}

// COMMAND ----------

def UpperCase(colName: String, df :DataFrame): DataFrame = {
   if(df.columns.contains(colName)){
     return df.withColumn(colName, upper(col(colName)));
   }
  else{
      Logger.getLogger("Such a column does not exist in df").setLevel(Level.ERROR)
      return spark.emptyDataFrame
  }
}

// COMMAND ----------

def fill_nan(colname: String, df :DataFrame): DataFrame = {
   if(df.columns.contains(colname)){
     return df.na.fill(0,Array(colname))
   }
  else
  {
      Logger.getLogger("Such a column does not exist in df").setLevel(Level.ERROR)
      return spark.emptyDataFrame
  }
  
}

// COMMAND ----------

def group_by(colname: String, df :DataFrame): DataFrame = {
   if(df.columns.contains(colname)){
     return df.groupBy(colname).count().sort($"count".desc)
   }
  else
  {
      Logger.getLogger("Such a column does not exist in df").setLevel(Level.ERROR)
      return spark.emptyDataFrame
  }
  
}
