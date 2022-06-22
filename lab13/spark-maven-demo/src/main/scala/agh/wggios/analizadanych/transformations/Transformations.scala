package agh.wggios.analizadanych.transformations

import agh.wggios.analizadanych.caseclass.FlightCaseClass
import org.apache.log4j.Logger
class Transformations {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

    def originIsDestination(flight_row: FlightCaseClass): Boolean = {
      logger.info("LOGGER INFO: TRANSFORMATION IN DATASET")
      return flight_row.ORIGIN_COUNTRY_NAME == flight_row.DEST_COUNTRY_NAME
    }
}
