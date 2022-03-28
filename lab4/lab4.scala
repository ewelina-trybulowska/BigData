// Databricks notebook source
// MAGIC %md 
// MAGIC Wykożystaj dane z bazy 'bidevtestserver.database.windows.net'
// MAGIC ||
// MAGIC |--|
// MAGIC |SalesLT.Customer|
// MAGIC |SalesLT.ProductModel|
// MAGIC |SalesLT.vProductModelCatalogDescription|
// MAGIC |SalesLT.ProductDescription|
// MAGIC |SalesLT.Product|
// MAGIC |SalesLT.ProductModelProductDescription|
// MAGIC |SalesLT.vProductAndDescription|
// MAGIC |SalesLT.ProductCategory|
// MAGIC |SalesLT.vGetAllCategories|
// MAGIC |SalesLT.Address|
// MAGIC |SalesLT.CustomerAddress|
// MAGIC |SalesLT.SalesOrderDetail|
// MAGIC |SalesLT.SalesOrderHeader|

// COMMAND ----------

//INFORMATION_SCHEMA.TABLES

val jdbcHostname = "bidevtestserver.database.windows.net"
val jdbcPort = 1433
val jdbcDatabase = "testdb"

val tabela = spark.read
  .format("jdbc")
  .option("url",s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}")
  .option("user","sqladmin")
  .option("password","$3bFHs56&o123$")
  .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
  .option("query","SELECT * FROM INFORMATION_SCHEMA.TABLES")
  .load()
display(tabela)

// COMMAND ----------

// MAGIC %md
// MAGIC 1. Pobierz wszystkie tabele z schematu SalesLt i zapisz lokalnie bez modyfikacji w formacie delta

// COMMAND ----------

val SalesLT = tabela.where("TABLE_SCHEMA == 'SalesLT'")

val names=SalesLT.select("TABLE_NAME").as[String].collect.toList

for( i <- names){
  val tab = spark.read
  .format("jdbc")
  .option("url",s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}")
  .option("user","sqladmin")
  .option("password","$3bFHs56&o123$")
  .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
  .option("query",s"SELECT * FROM SalesLT.$i")
  .load()
  
  tab.write.format("delta").mode("overwrite").saveAsTable(i)
  
}




// COMMAND ----------

 // data are stored in the DBFS root locations /user/hive/warehouse  ->Data and metadata for non-external Hive tables.
display(dbutils.fs.ls("dbfs:/user/hive/warehouse"))

// COMMAND ----------

// MAGIC %md
// MAGIC  Uzycie Nulls, fill, drop, replace, i agg
// MAGIC  * W każdej z tabel sprawdź ile jest nulls w rzędach i kolumnach
// MAGIC  * Użyj funkcji fill żeby dodać wartości nie występujące w kolumnach dla wszystkich tabel z null
// MAGIC  * Użyj funkcji drop żeby usunąć nulle, 
// MAGIC  * wybierz 3 dowolne funkcje agregujące i policz dla TaxAmt, Freight, [SalesLT].[SalesOrderHeader]
// MAGIC  * Użyj tabeli [SalesLT].[Product] i pogrupuj według ProductModelId, Color i ProductCategoryID i wylicz 3 wybrane funkcje agg() 
// MAGIC    - Użyj conajmniej dwóch overloded funkcji agregujących np z (Map)

// COMMAND ----------

// W każdej z tabel sprawdź ile jest nulls w rzędach i kolumnach
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.{col,when, count}
import org.apache.spark.sql.Column

//nulls w kolumnach:
def countCols(columns:Array[String]):Array[Column]={
    columns.map(c=>{
      count(when(col(c).isNull,c)).alias(c)
    })
}

val names_lower=names.map(x => x.toLowerCase())
for( i <- names_lower){
  val df = spark.read.format("delta")
              .option("header","true")
              .option("inferSchema","true")
              .load(s"dbfs:/user/hive/warehouse/$i")
  
  df.select(countCols(df.columns):_*).show()
  
}


// COMMAND ----------

//nulls w rzędach
val df = spark.read.format("delta")
              .option("header","true")
              .option("inferSchema","true")
              .load(s"dbfs:/user/hive/warehouse/customer")



import org.apache.spark.sql.functions._
val colnames=df.columns
val null_counter = colnames.map(x => when(col(x).isNull , 1).otherwise(0)).reduce(_+_)

val df2 = df.withColumn("nulls_cnt", null_counter)

display(df2)

// COMMAND ----------

//Użyj funkcji fill żeby dodać wartości nie występujące w kolumnach dla wszystkich tabel z null
for( i <- names_lower){
  val df = spark.read.format("delta")
              .option("header","true")
              .option("inferSchema","true")
              .load(s"dbfs:/user/hive/warehouse/$i")
  
 
  val colnames = df.columns
  val df1=df.na.fill("0", colnames)
  display(df1)
}

// COMMAND ----------

//Użyj funkcji drop żeby usunąć nulle
  val df = spark.read.format("delta")
              .option("header","true")
              .option("inferSchema","true")
              .load(s"dbfs:/user/hive/warehouse/customer")
  
  val drop_nulls = df.na.drop()
  display(drop_nulls)


// COMMAND ----------


//wybierz 3 dowolne funkcje agregujące i policz dla TaxAmt, Freight, [SalesLT].[SalesOrderHeader]
val SalesOrderHeader = spark.read.format("delta")
              .option("header","true")
              .option("inferSchema","true")
              .load(s"dbfs:/user/hive/warehouse/salesorderheader")

val agg_sum = SalesOrderHeader.agg(sum("TaxAmt").as("TaxSum"), sum("Freight").as("FreightSum"))
display(agg_sum)

// COMMAND ----------

val agg_mean = SalesOrderHeader.agg(mean("TaxAmt").as("TaxMean"), mean("Freight").as("FreightMean"))
display(agg_mean)

// COMMAND ----------

val agg_max = SalesOrderHeader.agg(max("TaxAmt").as("TaxMax"), max("Freight").as("FreightMax"))
display(agg_max)

// COMMAND ----------

import org.apache.spark.sql.functions.{first, last}
SalesOrderHeader.select(first("TaxAmt"), last("TaxAmt"),first("Freight"), last("Freight")).show()

// COMMAND ----------

import org.apache.spark.sql.functions.{corr, covar_pop, covar_samp}
SalesOrderHeader.select(corr("TaxAmt", "Freight"), covar_samp("TaxAmt", "Freight"),
    covar_pop("TaxAmt", "Freight")).show()

// COMMAND ----------

//Użyj tabeli [SalesLT].[Product] i pogrupuj według ProductModelId, Color i ProductCategoryID i wylicz 3 wybrane funkcje agg()
//Użyj conajmniej dwóch overloded funkcji agregujących np z (Map)

// COMMAND ----------

val product = spark.read.format("delta")
              .option("header","true")
              .option("inferSchema","true")
              .load(s"dbfs:/user/hive/warehouse/product")
val group1 = product.groupBy("Color").sum()
display(group1)

// COMMAND ----------

val group2= product.groupBy("ProductModelId", "Color", "ProductCategoryID").agg(Map(
  "StandardCost" -> "avg", 
  "Weight" -> "avg",
  "ListPrice" -> "min"
  ))
display(group2)

// COMMAND ----------

val group3= product.groupBy("ProductModelId", "Color", "ProductCategoryID").agg(Map(
  "Weight" -> "min", 
  "StandardCost" -> "max"
  ))
display(group3)

// COMMAND ----------

//Zadanie 2 
//Stwórz 3 funkcje UDF do wybranego zestawu danych, 
//Dwie funkcje działające na liczbach, int, double 
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.udf
val intUDF=udf((x:Int) => x+1 )
val doubleUDF=udf((x:Double) => x*0.75 )
//Jedna funkcja na string  
val stringUDF=udf((s: String) => s.toUpperCase )

val newDf=product.select(intUDF($"ProductID") as "ProductID+1", doubleUDF($"ListPrice") as "ListPrice*0.75",stringUDF($"Name") as "Upper name")
display(newDf)


// COMMAND ----------

//Zadanie 3 
//Flatten json, wybieranie atrybutów z pliku json.

val path = "dbfs:/FileStore/tables/brzydki.json"
val file=spark.read.option("multiLine", true).option("mode", "PERMISSIVE").json(path)
//file.printSchema()
display(file)

// COMMAND ----------

import spark.implicits._

//select geometry type:
val select_attr=file.select($"type",$"jobDetails",$"features")
        //.withColumn("jobId",$"jobDetails.jobID")
        //.withColumn("jobName",$"jobDetails.jobName")
        //.withColumn("changesTimestamp",$"jobDetails.changesTimestamp")
        //.withColumn("jobType",$"jobDetails.jobType")
        //.withColumn("workspaceId",$"jobDetails.workspaceId")
        .drop($"jobDetails")
        //.withColumn("geometry",explode($"features.geometry"))
        //.withColumn("features_type",$"features.type")
        //.select($"*",explode($"features.geometry.coordinates") as "geometry.coodinates")
        .withColumn("geometry_type",explode($"features.geometry.type"))
        .drop($"features")
        .drop($"geometry")

display(select_attr)

// COMMAND ----------

//select jobDetails
val select_attr=file.select($"type",$"jobDetails")
        .withColumn("jobId",$"jobDetails.jobID")
        .withColumn("jobName",$"jobDetails.jobName")
        .withColumn("changesTimestamp",$"jobDetails.changesTimestamp")
        .withColumn("jobType",$"jobDetails.jobType")
        .withColumn("workspaceId",$"jobDetails.workspaceId")
        .drop($"jobDetails")

display(select_attr)

// COMMAND ----------

//select features->geometry->coordinates
val select_attr3=file.select($"features")
                      .select($"*",explode($"features.geometry.coordinates") as "geometry.coodinates")
                      .drop($"features")
                     
       

display(select_attr3)
