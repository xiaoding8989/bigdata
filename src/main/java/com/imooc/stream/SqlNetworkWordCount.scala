package com.imooc.stream
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

/**
  * Spark Streaming整合Spark SQL完成词频统计操作
  */
object SqlNetworkWordCount {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val sparkConf = new SparkConf().setAppName("ForeachRDDApp").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val lines=ssc.socketTextStream("master",9999)
    val words=lines.flatMap(_.split(" "))
    val spark=SparkSession.builder().appName("SparkStatCleanJob")
      .config("spark.sql.warehouse.dir", "D:/scala_path/SparkStremBuChong/spark-warehouse")
      .config("spark.sql.sources.partitionColumnTypeInference.enabled","false")
      .master("local[2]").getOrCreate()
    import spark.implicits._
    println("*"*90)
    words.print()
    words.foreachRDD { (rdd: RDD[String], time: Time) =>



//      val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
//      import spark.implicits._


      val wordsDataFrame = rdd.map(w => Record(w)).toDF()


      wordsDataFrame.createOrReplaceTempView("words")


      val wordCountsDataFrame =
        spark.sql("select word, count(*) as total from words group by word")
      println(s"========= $time =========")
      wordCountsDataFrame.show()
    }

    ssc.start()
    ssc.awaitTermination()

  }

  case class Record(word: String)


  object SparkSessionSingleton {

    @transient  private var instance: SparkSession = _

    def getInstance(sparkConf: SparkConf): SparkSession = {
      if (instance == null) {
        instance = SparkSession
          .builder
          .config(sparkConf)
          .config("spark.sql.warehouse.dir", "D:/scala_path/SparkStremBuChong/spark-warehouse")
          .getOrCreate()
      }
      instance
    }
  }

}
