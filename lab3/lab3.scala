// Databricks notebook source
//Zadanie 1 
//Importuj notatnik Ćwiczenia.dbc i wykonaj opisane zadania Podczas wykonywania zadań sprawdź plan wykonania `explain()` 

// COMMAND ----------

//Zadanie 2 
//Przejdź przez Spark UI i opisz w kilku zdaniach co można znaleźć w każdym z elementów Spark UI. 

//1.Jobs-pokazuje poszczególne etapach zadania, datę i czas kiedy zadanie się rozpoczęło, czas wykonania

//2.Stages-pokazuje pojedyncze etapy każdego z zadań i ile zadań jest przetwarzanych przez wykonawców. 

//3.Storage-zawiera informacje dotyczące danych

//4.Environment-zawiera informacje o środowisku węzłów wykonawczych. Czyli najważniejsze informacje wersja Java, Scala i dane Spark.

//5.Executors(wykonawcy)-pokazuje informacje o węzłach wykonawczych. Każdy węzeł jest opisany i widać jakie ma zasoby.

//6.SQL- zawiera informacje dotyczące komend

//7.JDBC/ODBC Server- wszystkie dane dotyczące aktualnych połączeń JDBC.

// COMMAND ----------

//Zadanie 3 
//Do jednej z Dataframe dołóż transformacje groupBy i porównaj jak wygląda plan wykonania  
val filePath = "dbfs:/FileStore/tables/Files/movies.csv"
val namesDf = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)


val xx=namesDf.select($"title",$"country").explain(true)



// COMMAND ----------

val xx2=namesDf.select($"title",$"country").groupBy("country").count().explain(true)

// COMMAND ----------

//Zadanie 4 
//Pobierz dane z SQL Server przynajmniej jedna tabelę. Sprawdź jakie są opcje zapisu danych przy użyciu konektora jdbc. 
//Server bidevtestserver.database.windows.net 
//Baza testdb 
//User: sqladmin 
//Hasło: $3bFHs56&o123$  

val user = "sqladmin"
val password  ="$3bFHs56&o123$"

val jdbcDF = (spark.read.format("jdbc")
.option("url",  "jdbc:sqlserver://bidevtestserver.database.windows.net:1433;database=testdb")
.option("dbtable", "(SELECT * FROM information_schema.tables) as tmp")
.option("user", user)
.option("password", password) 
.option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") 
.load())

display(jdbcDF)
