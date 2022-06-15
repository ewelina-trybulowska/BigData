package agh.wggios.analizadanych

import agh.wggios.analizadanych.caseclass.FlightCaseClass
import agh.wggios.analizadanych.datareader.DataReader
import agh.wggios.analizadanych.sparksessionprovider.SparkSessionProvider
import agh.wggios.analizadanych.transformations.Transformations

object Main extends SparkSessionProvider{

  def main(args: Array[String]): Unit = {
    import spark.implicits._
    val df =new DataReader("2010-summary.csv").read_csv().as[FlightCaseClass]

    val xx =df.filter(flight_row => new Transformations().originIsDestination(flight_row)).show()
  }

}
