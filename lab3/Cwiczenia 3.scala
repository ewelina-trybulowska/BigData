// Databricks notebook source
// MAGIC %md Names.csv 
// MAGIC * Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
// MAGIC * Dodaj kolumnę w której wyliczysz wzrost w stopach (feet)
// MAGIC * Odpowiedz na pytanie jakie jest najpopularniesze imię?
// MAGIC * Dodaj kolumnę i policz wiek aktorów 
// MAGIC * Usuń kolumny (bio, death_details)
// MAGIC * Zmień nazwy kolumn - dodaj kapitalizaję i usuń _
// MAGIC * Posortuj dataframe po imieniu rosnąco

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.functions.col


// COMMAND ----------

val filePath = "dbfs:/FileStore/tables/Files/names.csv"
val start=System.currentTimeMillis()
val namesDf = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)
val end=System.currentTimeMillis()

display(namesDf)

// COMMAND ----------

//dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
val epoch_time=namesDf.withColumn("epoch_time", lit(end-start))
display(epoch_time)

// COMMAND ----------

//Dodaj kolumnę w której wyliczysz wzrost w stopach (feet)
val feet = namesDf.withColumn("Feet", ($"height"/30.48).as("Feet"))         
//display(feet)

// COMMAND ----------

//Odpowiedz na pytanie jakie jest najpopularniesze imię?
val get_name = namesDf.select(split(col("name")," ").getItem(0).as("Name"))
val df3=get_name.groupBy("Name").count().sort($"count".desc).show(1)

//najbardziej popularne imię to John



// COMMAND ----------

//Dodaj kolumnę i policz wiek aktorów
val ageDf = namesDf
  .withColumn("birth",to_date($"date_of_birth", "dd.MM.yyyy"))
  .withColumn("death",to_date($"date_of_death", "dd.MM.yyyy"))

val actors_age=ageDf.withColumn("date_difference", round(months_between(col("death"),col("birth"))/12))


display(actors_age)

// COMMAND ----------

//Usuń kolumny (bio, death_details)
val drop_columns=namesDf.drop("bio", "death_details")
//display(drop_columns)

// COMMAND ----------

//Rename columns-duża pierwsza litera, usunięcie _
val new_column_names=namesDf.columns.map(c=>c.split("_").map(_.capitalize).mkString(""))
val df3 = namesDf.toDF(new_column_names:_*)
display(df3)



// COMMAND ----------

//Posortuj dataframe po imieniu rosnąco
val sort_by_name=namesDf.sort(col("name").asc)
display(sort_by_name)

// COMMAND ----------

// MAGIC %md Movies.csv
// MAGIC * Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
// MAGIC * Dodaj kolumnę która wylicza ile lat upłynęło od publikacji filmu
// MAGIC * Dodaj kolumnę która pokaże budżet filmu jako wartość numeryczną, (trzeba usunac znaki walut)
// MAGIC * Usuń wiersze z dataframe gdzie wartości są null

// COMMAND ----------

val filePath = "dbfs:/FileStore/tables/Files/movies.csv"
val start=System.currentTimeMillis()
val namesDf = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)
val end=System.currentTimeMillis()

display(namesDf)

// COMMAND ----------

//dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
val epoch_time=namesDf.withColumn("epoch_time", lit(end-start))
display(epoch_time)

// COMMAND ----------

//Dodaj kolumnę która wylicza ile lat upłynęło od publikacji filmu
val df = namesDf
  .withColumn("current_date",current_date())
  .withColumn("date_published", to_date($"date_published", "dd.MM.yyyy"))
  .withColumn("years_since_film_publication",round(datediff(col("current_date"),col("date_published"))/365))
display(df)

// COMMAND ----------

//Dodaj kolumnę która pokaże budżet filmu jako wartość numeryczną, (trzeba usunac znaki walut)
val new_budget=namesDf.withColumn("numeric_budget", regexp_replace(col("budget"), "[^0-9]", ""));
display(new_budget)

// COMMAND ----------

//usunąć wiersze z Na
val df_without_NA=namesDf.na.drop()
display(df_without_NA)

// COMMAND ----------

// MAGIC %md ratings.csv
// MAGIC * Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
// MAGIC * Dla każdego z poniższych wyliczeń nie bierz pod uwagę `nulls` 
// MAGIC * Dodaj nowe kolumny i policz mean i median dla wartości głosów (1 d 10)
// MAGIC * Dla każdej wartości mean i median policz jaka jest różnica między weighted_average_vote
// MAGIC * Kto daje lepsze oceny chłopcy czy dziewczyny dla całego setu
// MAGIC * Dla jednej z kolumn zmień typ danych do `long` 

// COMMAND ----------

val filePath = "dbfs:/FileStore/tables/Files/ratings.csv"
val namesDf = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)

display(namesDf)

// COMMAND ----------

val df=namesDf.na.drop()

// COMMAND ----------

//Dodaj nowe kolumny i policz mean i median dla wartości głosów (1 d 10)
//to jest już zrobione w tabeli wjściowej
val selected_cols_names = df.columns.toList.slice(5,15)

val votes_cols = df.select(selected_cols_names.map(c => col(c)): _*)
val mean_votes = df.withColumn("mean_votes[1-10]", votes_cols.columns.map(c => col(c)).reduce((c1, c2) => c1 + c2) / lit(votes_cols.columns.length))
//display(mean_votes_cols)



// COMMAND ----------

//Dla każdej wartości mean i median policz jaka jest różnica między weighted_average_vote
val df2=df.withColumn("|mean - weighted_average_vote|",abs($"mean_vote"-$"weighted_average_vote")).withColumn("|median - weighted_average_vote|",abs($"median_vote"-$"weighted_average_vote"))
display(df2)

// COMMAND ----------

//Kto daje lepsze oceny chłopcy czy dziewczyny dla całego setu
namesDf.describe("males_allages_avg_vote").show()
namesDf.describe("females_allages_avg_vote").show()
//odp. lepsze oceny dla calego setu mają mężczyźni

// COMMAND ----------

//Dla jednej z kolumn zmień typ danych do long
val cast=namesDf.select(col("mean_vote").cast(LongType).as("long_mean_vote"))
cast.printSchema

