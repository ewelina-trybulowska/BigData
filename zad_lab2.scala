// Databricks notebook source
//Zadanie 1. 
//Wybierz jeden z plików csv z poprzednich ćwiczeń i stwórz ręcznie schemat danych. Stwórz DataFrame wczytując plik z użyciem schematu. 
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

//Zadanie 2 
//Użyj kilku rzędów danych z jednego z plików csv i stwórz plik json. Stwórz schemat danych do tego pliku. Przydatny tool to sprawdzenie formatu danych. https://jsonformatter.curiousconcept.com/ 
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

// Convenience function for turning JSON strings into DataFrames.
def jsonToDataFrame(json: String, schema: StructType = null): DataFrame = {
  // SparkSessions are available with Spark 2.0+
  val reader = spark.read
  Option(schema).foreach(reader.schema)
  reader.json(sc.parallelize(Array(json)))
}

  val zad2 = jsonToDataFrame("""
[
 {
       
    "imdb_title_id":"tt0000009",
    "ordering":1,
    "imdb_name_id":"nm0063086",
    "category":"actress",
    "job":"null",
    "characters":["Miss Geraldine Holbrook (Miss Jerry)"]
    },
    
    {
       "imdb_title_id":"tt0000009",
    "ordering":2,
    "imdb_name_id":"nm0183823",
    "category":"actor",
    "job":"null",
    "characters":["Mr. Hamilton"]
      
    } 
  
]
""", ActorsSchema)
 
//display(zad2.select("*"))

zad2.write.json("dbfs:/FileStore/tables/zad2.json")
val actors_with_schema = spark.read.schema(ActorsSchema).json("dbfs:/FileStore/tables/zad2.json")
display(actors_with_schema)



// COMMAND ----------

//Zadanie 3 
//Pobierz notatnik Daty.dbc, użyj funkcji i dokonaj transformacji dat i czasu jak w przykładach. 



// COMMAND ----------

//Zadanie 4 
//Użycie Read Modes.  
//Wykorzystaj posiadane pliki bądź dodaj nowe i użyj wszystkich typów oraz ‘badRecordsPath’, zapisz co się dzieje. Jeśli jedna z opcji nie da żadnych efektów, trzeba popsuć dane. 
val w1 = "{error}"

val w2 = "{'imdb_title_id':'tt0000009','ordering':1,'imdb_name_id':'nm0063086','category':'actress','job':'null','characters':'Miss Geraldine Holbrook (Miss Jerry)'}"

val w3 = "{ 'imdb_title_id':'tt0000009','ordering':aaaaaa,'imdb_name_id':'nm0183823','category':'actor','job':'null','characters':'Mr. Hamilton'}"

Seq(w1, w2, w3).toDF().write.mode("overwrite").text("/FileStore/tables/zad4.json")


val badRecordsPath = spark.read.format("json")//bledne wiersze zostja przekierowane do sciezki z bledami a poprawne wiersze sie wypisuja
  .schema(ActorsSchema)
  .option("badRecordsPath", "/FileStore/tables/badrecords")
  .load("/FileStore/tables/zad4.json")

val permissive = spark.read.format("json") //jesli jakis atrybut nie moze zostac wczytany jest bledny to Spark zamienia caly wiersz na null
  .schema(ActorsSchema)
  .option("mode", "PERMISSIVE")
  .load("/FileStore/tables/zad4.json")

val DropMalFormed = spark.read.format("json") //usuwa wiersze z bledami
  .schema(ActorsSchema)
  .option("mode", "DROPMALFORMED")
  .load("/FileStore/tables/zad4.json")

val FailFast = spark.read.format("json") //jak jest blad proces odczytu zostaje calkowicie zatrzymany
  .schema(ActorsSchema)
  .option("mode", "FAILFAST")
  .load("/FileStore/tables/zad4.json")

display(permissive)
//display(dbutils.fs.ls("dbfs:/FileStore/tables/badrecords/"))


// COMMAND ----------

//Zadanie 5 
//Użycie DataFrameWriter. 
//Zapisz jeden z wybranych plików do formatów (‘.parquet’, ‘.json’). Sprawdź, czy dane są zapisane poprawnie, użyj do tego DataFrameReader. Opisz co widzisz w docelowej ścieżce i otwórz używając //DataFramereader. 

val filePath = "dbfs:/FileStore/tables/Files/actors.csv"
val file=spark.read.format("csv") 
            .option("header","true") 
            .schema(ActorsSchema) 
            .load(filePath)
file.write.format("parquet").mode("overwrite").save("/FileStore/tables/actors_parquet.parquet")
val actors_parquet = spark.read.format("parquet").load("/FileStore/tables/actors_parquet.parquet")
display(actors_parquet)

//

// COMMAND ----------

file.write.format("json").mode("overwrite").save("/FileStore/tables/actors_json.json")
val actors_json = spark.read.format("json").load("/FileStore/tables/actors_json.json")
display(actors_json)

// COMMAND ----------

display(dbutils.fs.ls("/FileStore/tables/actors_parquet.parquet"))

// COMMAND ----------

display(dbutils.fs.ls("/FileStore/tables/actors_json.json"))

// COMMAND ----------

//po otwarciu docelowej sciezki w obu przypadkach widze kilkanascie plikow, roznica jest taka ze pliki parquet maja mniejszy rozmiar od plikow json
