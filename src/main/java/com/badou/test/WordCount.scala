package com.badou.test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\develop\\hadoop-common-2.2.0-bin-master")
    val conf = new SparkConf().setAppName("wc").setMaster("local[2]")
    val sc = new SparkContext(conf)
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val textRdd = sc.textFile("D:\\data\\data\\a.txt")
    //textRdd.flatMap(line => line.split(" ")).map(line => (line, 1)).reduceByKey(_ + _).sortBy(_._2, ascending = false).take(20).foreach(println)
    textRdd.flatMap(line=>line.split(" ")).map(line=>(line,1)).reduceByKey(_+_).sortBy(_._2,ascending = false).take(20).foreach(println)

  }
}
