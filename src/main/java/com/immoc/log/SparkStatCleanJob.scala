package com.immoc.log

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkStatCleanJob {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val spark=SparkSession.builder().appName("SparkStatCleanJob")
      .config("spark.sql.warehouse.dir", "D:/scala_path/SparkStremBuChong/spark-warehouse")
      .config("spark.sql.parquet.compression.codec","gzip")
      .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
      .master("local[2]").getOrCreate()
    val accessRDD=spark.sparkContext.textFile("file:///D:/bigdata/access.log")

    val accessDF=spark.createDataFrame(accessRDD.map(x=>AccessConvertUtil.parseLog(x)).filter(data=>data.length !=1),
      AccessConvertUtil.struct
    )
//    accessDF.printSchema()
//    accessDF.show(false)


      accessDF.coalesce(1).write.format("parquet").mode(SaveMode.Overwrite)
        .partitionBy("day").save("file:///D:/data/imooc/clean")


    spark.stop()


  }

}
