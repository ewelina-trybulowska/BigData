package agh.wggios.analizadanych.transformations

import agh.wggios.analizadanych.caseclass.FlightCaseClass

class Transformations {

    def originIsDestination(flight_row: FlightCaseClass): Boolean = {
      return flight_row.ORIGIN_COUNTRY_NAME == flight_row.DEST_COUNTRY_NAME
    }
}
