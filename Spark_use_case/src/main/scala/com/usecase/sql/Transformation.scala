package com.usecase.sql

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, concat, concat_ws, count, date_trunc, dense_rank, lit, max, rank, row_number, sum}

object Transformation {
  //def addLiteralColumn(data: DataFrame): DataFrame = {
    //data.select(col("*"), lit(1).alias("literal"))
  //}
  def createTop(data: DataFrame): DataFrame = {
    val dfFiltered = data.select("noc").groupBy("noc")
    val total=data.count()
      val windowTop = Window.orderBy(col("records").desc)
    val dfrecords= dfFiltered.agg(count("noc").as("records"))
      .withColumn("total_records",lit(total).alias("total_records"))
    dfrecords.withColumn("rank", row_number.over(windowTop))
  }
 def getTableFiltered(data:DataFrame):DataFrame={
   data.filter(col("noc") === "Japan")


 }
  def createPercent (data:DataFrame):DataFrame={
    val dfpercent=data.filter(col("noc") === "Japan")
    dfpercent.withColumn("percent_records", ((col("records")*100))/ col ("total_records"))


  }

  def createMessage (data:DataFrame):DataFrame={
    data.withColumn("message", lit(s"japan in place ${data.first().getAs[Int]("rank")}  " +
      s"with ${data.first().getAs[Int]("total")} won medals!"  ))
  }


  def joinObjetcs(data: DataFrame,data1: DataFrame,data2: DataFrame): DataFrame = {
    val joines = data.join(data1,"noc").join(data2,"noc")
    joines.select("*")

  }






}
