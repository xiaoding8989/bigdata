package com.badou.test

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSql {
  def main(args: Array[String]): Unit = {
    //val sc = new SparkConf()
    //val spark = SparkSession.builder().appName("hive test").master("local").enableHiveSupport().getOrCreate()
    //val df = spark.sql("select * from default")
    //df.show()

    val spark= SparkSession.builder.master("local")
      .appName("spark session example")
      .enableHiveSupport()
      .getOrCreate()
    spark.sql("set hive.cli.print.header=true")
    //    System.exit(1)

    spark.sql("show tables").show()
  }

}
