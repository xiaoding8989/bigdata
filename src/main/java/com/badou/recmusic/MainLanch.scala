package com.badou.recmusic

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object MainLanch {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val spark = SparkSession.builder()
      .appName("HBase Write From Spark")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "D:\\scala_path\\SparkStremBuChong\\spark-warehouse")
      .enableHiveSupport()
      .getOrCreate()
      val df_tmp_d=spark.sql("select user_id from badou.tmp_d")
    val df_tmp=df_tmp_d.selectExpr("user_id as user_id1")
    val df_d=df_tmp_d.join(df_tmp).filter("cast(user_id as long)< cast(user_id1 as long)")
    df_d.show()




  }

}
