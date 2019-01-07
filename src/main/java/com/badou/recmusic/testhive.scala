package com.badou.recmusic

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object testhive {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val ZOOKEEPER_QUORUM = "192.168.95.5,192.168.95.4,192.168.95.3"
    val spark = SparkSession.builder()
      .appName("HBase Write From Spark")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "D:\\scala_path\\SparkStremBuChong\\spark-warehouse")
      .enableHiveSupport()
      .getOrCreate()
    val rdd = spark.sql(
      """
        |select
        |product_id,
        |product_name,
        |aisle_id,
        |department_id
        |from badou.products
        |where product_id='12'
      """.stripMargin).rdd
    rdd.map{row=>
      val product_id=row(0).asInstanceOf[String]
      val product_name=row(1).asInstanceOf[String]
      val aisle_id=row(2).asInstanceOf[String]
      val department_id=row(3).asInstanceOf[String]
      val p=new Put(Bytes.toBytes(product_id))
      p.add(Bytes.toBytes("id"),Bytes.toBytes("aisle_id"),Bytes.toBytes(aisle_id))
      p.add(Bytes.toBytes("id"),Bytes.toBytes("department_id"),Bytes.toBytes(department_id))
      p.add(Bytes.toBytes("name"),Bytes.toBytes("product_name"),Bytes.toBytes(product_name))
      p

    }.foreachPartition { Iterator =>
      val jobConf = new JobConf(HBaseConfiguration.create())
      jobConf.set("hbase.zookeeper.quorum", ZOOKEEPER_QUORUM)
      jobConf.set("hbase.zookeeper.property.clientPort", "2181")
      jobConf.set("zookeeper.znode.parent", "/hbase")
      jobConf.setOutputFormat(classOf[TableOutputFormat])


      val table = new HTable(jobConf, TableName.valueOf("products"))
      import scala.collection.JavaConversions._
      table.put(seqAsJavaList(Iterator.toSeq))

    }

  }



}
