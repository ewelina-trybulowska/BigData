package agh.wggios.analizadanych

import agh.wggios.analizadanych.caseclass.FlightCaseClass
import agh.wggios.analizadanych.datareader.DataReader
import agh.wggios.analizadanych.sparksessionprovider.SparkSessionProvider
import agh.wggios.analizadanych.transformations.Transformations
import agh.wggios.analizadanych.datawriter.DataWriter
object Main extends SparkSessionProvider{

  def main(args: Array[String]): Unit = {
    import spark.implicits._
    logger.info("START ")

    val df =new DataReader(args(0)).read().as[FlightCaseClass]
    val xx =df.filter(flight_row => new Transformations().originIsDestination(flight_row)).show()
  }

}
