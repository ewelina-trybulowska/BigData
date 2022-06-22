package agh.wggios.analizadanych.datareader
import agh.wggios.analizadanych.sparksessionprovider.SparkSessionProvider
import org.apache.spark.sql.DataFrame

import java.nio.file.Files
import java.nio.file.Paths
import org.apache.log4j.Logger

class DataReader(path:String) extends SparkSessionProvider {

  def ifExists() : Boolean = {
    return Files.exists(Paths.get(this.path))
  }

  def getExtension : String = {
    val fileName = Paths.get(path).getFileName
    val extension = fileName.toString.split("\\.").last
    extension
  }

  def read(): DataFrame ={
    logger.info("reading file")

    if (ifExists()){
      spark.read.format(getExtension).
        option("inferSchema",true).
        option("header",true).
        load(this.path)
    } else {
      logger.warn("File not found: " + this.path)
      System.exit(0)
      spark.emptyDataFrame
    }
  }
}
