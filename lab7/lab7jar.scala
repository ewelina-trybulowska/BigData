import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, upper}

object lab7 extends App {

  val spark = SparkSession.builder
    .master("local[4]")
    .appName("Moja-applikacja")
    .getOrCreate()

  import java.net.URL
  import org.apache.spark.SparkFiles
  val urlfile="https://raw.githubusercontent.com/cegladanych/azure_bi_data/main/IMDB_movies/actors.csv"
  spark.sparkContext.addFile(urlfile)

  val df = spark.read
    .option("inferSchema", true)
    .option("header", true)
    .csv("file:///"+SparkFiles.get("actors.csv"))



  val df1=df.withColumn("category",upper(col("category")))
  val df2=df1.withColumn("ordering*2",col("ordering")*2)
  val df3=df2.drop(col("job"))
  df3.show(20)


}
