// Databricks notebook source
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// COMMAND ----------

//Zadanie 1 
//Czy Hive wspiera indeksy? Jeśli staniesz przed problemem – czy da się szybko wyciągać dane? 
//Odpowiedz na pytanie i podaj przykłady jak rozwiążesz problem indeksów w Hive. 


// COMMAND ----------

// MAGIC %md
// MAGIC //https://www.oreilly.com/library/view/programming-hive/9781449326944/ch08.html
// MAGIC 
// MAGIC Hive ma ograniczone możliwości indeksowania. Nie ma kluczy w zwykłym sensie relacyjnej bazy danych, ale można utworzyć indeks na kolumnach, aby przyspieszyć niektóre operacje. Dane indeksu dla tabeli są przechowywane w innej tabeli.
// MAGIC 
// MAGIC Celem indeksowania Hive jest zwiększenie szybkości wyszukiwania zapytań w niektórych kolumnach tabeli. Bez indeksu zapytania z predykatami, takimi jak "WHERE tab1.col1 = 10", ładują całą tabelę lub partycję i przetwarzają wszystkie wiersze. Ale jeśli istnieje indeks dla col1, to tylko część pliku musi zostać załadowana i przetworzona.
// MAGIC 
// MAGIC Poprawa szybkości zapytań, którą może zapewnić indeks, wiąże się z kosztem dodatkowego przetwarzania w celu utworzenia indeksu i miejsca na dysku do przechowywania indeksu.
// MAGIC 
// MAGIC Indeksy w Hive, podobnie jak te w relacyjnych bazach danych, muszą być dokładnie ocenione. Utrzymywanie indeksu wymaga dodatkowego miejsca na dysku, a tworzenie indeksu wiąże się z kosztami przetwarzania. Użytkownik musi porównać te koszty z korzyściami, jakie oferują podczas wykonywania zapytań do tabeli. Np.Jeśli długi czas trwania analizy nam nie przeszkadza, ale mamy ograniczoną ilość zasobów/miejsca na dysku nie powinniśmy używać indeksów.

// COMMAND ----------

//Zadanie 2 
//Stwórz diagram draw.io pokazujący jak powinien wyglądać pipeline. Wymysł kilka transfomracji, np. usunięcie kolumny, wyliczenie współczynnika dla x gdzie wartość do formuły jest w pliku //referencyjnym 
//Wymagania: 
//Ilość źródeł: 7; 5 rodzajów plików, 2 bazy danych,  

// COMMAND ----------

//Zadanie 3  
//Napisz funkcję, która usunie danych ze wszystkich tabel w konkretniej bazie danych. Informacje pobierz z obiektu Catalog. 

// COMMAND ----------

spark.catalog.listDatabases().show()

// COMMAND ----------

//create database
spark.sql("CREATE DATABASE lab7")

// COMMAND ----------

//create tables:
val filePath = "dbfs:/FileStore/tables/names.csv"
val df = spark.read.format("csv")
    .option("header","true")
    .option("inferSchema","true")
    .load(filePath)

df.write.mode("overwrite").saveAsTable("lab7.names")


// COMMAND ----------

val filePath2 = "dbfs:/FileStore/tables/actors.csv"
val df = spark.read.format("csv")
    .option("header","true")
    .option("inferSchema","true")
    .load(filePath2)

df.write.mode("overwrite").saveAsTable("lab7.actors")

// COMMAND ----------

spark.catalog.listTables("lab7").show()

// COMMAND ----------

val xx=spark.sql(s"SELECT * FROM lab7.actors")
xx.show()

// COMMAND ----------

//funkcja do usuwania danych:

def drop_data(database: String){
  
  val tables = spark.catalog.listTables(s"$database")
  val tables_names = tables.select("name").as[String].collect.toList
  var i = List()
  
  for( i <- tables_names){
    spark.sql(s"DELETE FROM $database.$i")
    print(f"Data from table $i deleted   ")
  }
}


// COMMAND ----------

drop_data("lab7")

// COMMAND ----------

val check=spark.sql(s"SELECT * FROM lab7.actors")
check.show()

// COMMAND ----------

//Zadanie 4 
//Stwórz projekt w IntelliJ z spakuj do go jar. Uruchom go w IntelliJ. 
//Uruchom w Databricks. 
//Aplikacja powinna wczytać plik tekstowy i wykonać kilka transformacji (np. dodaj kolumnę, zmień wartości kolumny ect). 
