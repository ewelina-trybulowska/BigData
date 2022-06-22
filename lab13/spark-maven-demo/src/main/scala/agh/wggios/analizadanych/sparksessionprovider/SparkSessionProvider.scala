package agh.wggios.analizadanych.sparksessionprovider

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

trait SparkSessionProvider {

  val spark = SparkSession
    .builder
    .master("local[4]")
    .appName("spark-cruncher")
    .getOrCreate()
  def getSparkSession():SparkSession = {return spark}

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

}