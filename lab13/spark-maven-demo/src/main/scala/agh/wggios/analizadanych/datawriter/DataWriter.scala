package agh.wggios.analizadanych.datawriter
import agh.wggios.analizadanych.sparksessionprovider.SparkSessionProvider
import org.apache.spark.sql.DataFrame

import java.nio.file.{Files, Paths}

class DataWriter(path:String,df:DataFrame) extends SparkSessionProvider {
  if(!this.df.isEmpty) {
    if(Files.exists(Paths.get(path))){
      logger.error("path file: \" +path + \" already exists.")

      System.exit(0)
    }
    logger.info("Saving to csv file")

    this.df.write.csv(path)
  } else{
    logger.error("dataframe is empty")

    System.exit(0)
  }
}
