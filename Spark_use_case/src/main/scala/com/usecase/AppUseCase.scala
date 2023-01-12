package com.usecase

import com.usecase.sql.Transformation
import com.usecase.utils.AppSession

object AppUseCase extends AppSession {

  def run(): Unit = {

    /**
     * INPUTS
     */
    logger.info("=====> Reading file")

    val dfAthletes = readParquet("input.pathAthletes")
    val dfCoaches = readParquet("input.pathCoaches")
    val dfMedals = readParquet("input.pathMedals")

    /**
     * TRANSFORMATIONS
     */
    logger.info("=====> Transforming data")
    //val dfAthletesWithExtraColumn = Transformation.addLiteralColumn(dfAthletes)
   // dfAthletesWithExtraColumn.show()

    //metodo 1 createTop
    val dftopAthletes = Transformation.createTop(dfAthletes)
    //dftopAthletes.show()

    val dftopCoaches = Transformation.createTop(dfCoaches)
    //dftopCoaches.show()

    //----------------------------------------------
    //metodo 2  getTableFiltered ---- filtrado medals
    val dfmedalsFiltered = Transformation.getTableFiltered(dfMedals)
    //dfmedalsFiltered.show()

    // filtrado topAthletes
    val dfathletesFiltered  = Transformation.getTableFiltered(dftopAthletes)
    //dfathletesFiltered.show()

    // filtrado topCoaches '---
    val dfCoachesFiltered = Transformation.getTableFiltered(dftopCoaches)
    //dfCoachesFiltered.show()

    //----------------------------------------------
    //metodo 3 createPercent
    val dfathletesResume= Transformation.createPercent(dfathletesFiltered)
    //dfathletesResume.show()

    val dfcoachesResume = Transformation.createPercent(dfCoachesFiltered)
    //dfcoachesResume.show()
    //----------------------------------------------
    //metodo 4 createMessage
    val dfmessage = Transformation.createMessage(dfmedalsFiltered)
    dfmessage.show()
    //----------------------------------------------
    //metodo 5 joinObjetcs
    val dfjoin = Transformation.joinObjetcs(dfmedalsFiltered,dfathletesResume,dfcoachesResume)
    dfjoin.show()
    /**
     * OUTPUT
     */

 writeParquet(dfjoin, "output.path")
    spark.stop()
    logger.info("=====> end process")

  }
}
