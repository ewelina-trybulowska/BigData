// Databricks notebook source
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, DateType,StringType, IntegerType,DecimalType, StructField, StructType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import spark.implicits._
import org.apache.spark.sql.functions.broadcast

// COMMAND ----------

//Zadanie 1
//Wykorzystaj przykłady z notatnika Windowed Aggregate Functions i przepisz funkcje używając Spark API

// COMMAND ----------

spark.sql("create database if not exists Sample")

// COMMAND ----------

val schema_transactions = new StructType()
  .add(StructField("AccountId", IntegerType, true))
  .add(StructField("TranDate", StringType, true))
  .add(StructField("TranAmt", DoubleType, true))

val schema_logical = new StructType()
  .add(StructField("RowID", IntegerType, true))
  .add(StructField("FName", StringType, true))
  .add(StructField("Salary", IntegerType, true))



// COMMAND ----------

val data_transactions=Seq(Row( 1, "2011-01-01", 500.0),
        Row( 1, "2011-01-15", 50.0),
        Row( 1, "2011-01-22", 250.0),
        Row( 1, "2011-01-24", 75.0),
        Row( 1, "2011-01-26", 125.0),
        Row( 1, "2011-01-28", 175.0),
        Row( 2, "2011-01-01", 500.0),
        Row( 2, "2011-01-15", 50.0),
        Row( 2, "2011-01-22", 25.0),
        Row( 2, "2011-01-23", 125.0),
        Row( 2, "2011-01-26", 200.0),
        Row( 2, "2011-01-29", 250.0),
        Row( 3, "2011-01-01", 500.0),
        Row( 3, "2011-01-15", 50.0),
        Row( 3, "2011-01-22", 5000.0),
        Row( 3, "2011-01-25", 550.0),
        Row( 3, "2011-01-27", 950.0),
        Row( 3, "2011-01-30", 2500.0))
  

// COMMAND ----------

val data_logical=Seq(Row(1,"George", 800),
        Row(2,"Sam", 950),
        Row(3,"Diane", 1100),
        Row(4,"Nicholas", 1250),
        Row(5,"Samuel", 1250),
        Row(6,"Patricia", 1300),
        Row(7,"Brian", 1500),
        Row(8,"Thomas", 1600),
        Row(9,"Fran", 2450),
        Row(10,"Debbie", 2850),
        Row(11,"Mark", 2975),
        Row(12,"James", 3000),
        Row(13,"Cynthia", 3000),
        Row(14,"Christopher", 5000))

// COMMAND ----------

val tmp = spark.sparkContext.parallelize(data_transactions)
val transactions = spark.createDataFrame(tmp, schema_transactions)
display(transactions)


// COMMAND ----------

val tmp2 = spark.sparkContext.parallelize(data_logical)
val logical = spark.createDataFrame(tmp2, schema_logical)
display(logical)

// COMMAND ----------

val window = Window.partitionBy("AccountId").orderBy("TranDate")
val df=transactions.withColumn("RunTotalAmt",sum("TranAmt").over(window)).orderBy("AccountId", "TranDate")
display(df)


// COMMAND ----------

val window = Window.partitionBy("AccountId").orderBy("TranDate")
val df2=transactions
.withColumn("RunAvg",avg("TranAmt").over(window))
.withColumn("RunTranQty",count("*").over(window))
.withColumn("RunSmallAmt",min("TranAmt").over(window))
.withColumn("RunLargeAmt",max("TranAmt").over(window))
.withColumn("RunTotalAmt",sum("TranAmt").over(window))
.orderBy("AccountId", "TranDate")

display(df2)

// COMMAND ----------

val window = Window.partitionBy("AccountId").orderBy("TranDate").rowsBetween(-2,Window.currentRow)
val window2 = Window.partitionBy("AccountId").orderBy("TranDate")
val df3=transactions
.withColumn("SlideAvg",avg("TranAmt").over(window))
.withColumn("SlideQty",count("*").over(window))
.withColumn("SlideMin",min("TranAmt").over(window))
.withColumn("SlideMax",max("TranAmt").over(window))
.withColumn("SlideTotal",sum("TranAmt").over(window))
.withColumn("RN", row_number().over(window2))
.orderBy("AccountId", "TranDate")

display(df3)

// COMMAND ----------

val window = Window.orderBy("Salary").rangeBetween(Window.unboundedPreceding, Window.currentRow)
val window2 = Window.orderBy("Salary").rowsBetween(Window.unboundedPreceding, Window.currentRow)

val df4=logical.withColumn("SumByRows", sum("salary").over(window2))
.withColumn("SumByRange", sum("salary").over(window))
.orderBy("RowID")

display(df4)

// COMMAND ----------

val window = Window.partitionBy("TranAmt").orderBy("TranDate")

val df5=transactions.withColumn("RN",row_number().over(window))
.orderBy("TranAmt")
.head(10)

display(df5)

// COMMAND ----------

//Zadanie 2
//Użyj ostatnich danych i użyj funkcji okienkowych LEAD, LAG, FIRST_VALUE, LAST_VALUE, ROW_NUMBER i DENS_RANK
//Każdą z funkcji wykonaj dla ROWS i RANGE i BETWEEN

//LAG --> zapewnia dostęp do wiersza o podanym przesunięciu poprzedzającym bieżący wierszu
//LEAD -->zapewnia dostęp do wiersza o określonym przesunięciu, które następuje po bieżącym 
//LAST_VALUE -->zwraca ostatnią wartość z uporządkowanego zestawu wartości
//FIRST_VALUE--> zwraca pierwszą wartość z uporządkowanego zestawu wartości
//ROW_NUMBER-->zwraca numer sekwencyjny zaczynając od 1 w obrębie partycji okna



// COMMAND ----------

val window = Window.partitionBy("AccountId").orderBy("TranAmt")
val window_range = Window.partitionBy("AccountId").orderBy("TranAmt").rangeBetween(-1, Window.currentRow)
val window_rows=Window.partitionBy("AccountId").orderBy("TranAmt").rowsBetween(Window.unboundedPreceding, -2)

val df=transactions.withColumn("Lead", lead(col("TranAmt"),1).over(window))
.withColumn("Lag", lag(col("TranAmt"),1).over(window))
.withColumn("first_range", first("TranAmt").over(window_range))
.withColumn("first_rows", first("TranAmt").over(window_rows))
.withColumn("last_range", last("TranAmt").over(window_range))
.withColumn("last_rows", last("TranAmt").over(window_rows))
.withColumn("row_number",row_number().over(window))
.withColumn("dense_rank",dense_rank().over(window))

display(df)

// COMMAND ----------

//Zadanie 3 
//Użyj ostatnich danych i wykonaj połączenia Left Semi Join, Left Anti Join, za każdym razem sprawdź .explain i zobacz jak spark wykonuje połączenia. Jeśli nie będzie danych to trzeba je //zmodyfikować żeby zobaczyć efekty.


// COMMAND ----------

val left_semi_join=logical.join(transactions,logical("RowID") ===  transactions("AccountId"),"leftsemi")
//display(left_semi_join)
left_semi_join.explain()

// COMMAND ----------

val left_anti_join=logical.join(transactions,logical("RowID") ===  transactions("AccountId"),"leftanti")
//display(left_anti_join)
left_anti_join.explain()

// COMMAND ----------

//Zadanie 4
//Połącz tabele po tych samych kolumnach i użyj dwóch metod na usunięcie duplikatów. 


//column rename
val transactions2 = transactions.withColumnRenamed("AccountId","RowID")
val join=logical.join(transactions2,logical("RowID") ===  transactions2("RowID"))
display(join)

// COMMAND ----------

val first_method=logical.join(transactions2,logical("RowID") ===  transactions2("RowID")).drop( transactions2("RowID"))
display(first_method)

// COMMAND ----------

val second_method=logical.join(transactions2,Seq("RowID") )
display(second_method)

// COMMAND ----------

//Zadanie 5 
//W jednym z połączeń wykonaj broadcast join, i sprawdź plan wykonania

// COMMAND ----------

val broadcast_join=logical.join(broadcast(transactions),logical("RowID")===transactions("AccountId"))

broadcast_join.explain()
//display(broadcast_join)
