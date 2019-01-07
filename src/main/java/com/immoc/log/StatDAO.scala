package com.immoc.log
import java.sql.{PreparedStatement,Connection}
import scala.collection.mutable.ListBuffer

object StatDAO {
  def insertDayVideoTrafficsAccessTopN(list:ListBuffer[DayVideoTrafficsStat]):Unit={
   var connection:Connection=null
    var pstmt:PreparedStatement=null

    try{
     connection=MySQLUtils.getConnection()
     connection.setAutoCommit(false)
     val sql="insert into day_video_traffics_topn_stat (day,cms_id,traffics) values (?,?,?)"
     pstmt=connection.prepareStatement(sql)

     for(ele<-list){
      pstmt.setString(1,ele.day)
      pstmt.setLong(2,ele.cmsId)
      pstmt.setLong(3,ele.traffics)
      pstmt.addBatch()

     }
     pstmt.executeBatch()
     connection.commit()

    }
    catch{
      case e:Exception=>e.printStackTrace()

    }finally {
      MySQLUtils.release(connection,pstmt)
    }

  }

 def insertvideoAccessTopNStat(list:ListBuffer[DayVideoAccessStat]):Unit={
  var connection:Connection=null
  var pstmt:PreparedStatement=null
  try{
   connection=MySQLUtils.getConnection()
   connection.setAutoCommit(false)
   val sql="insert into day_video_access_topn_stat (day,cms_id,times) values (?,?,?)"
   pstmt=connection.prepareStatement(sql)
   for(ele <- list){
    pstmt.setString(1,ele.day)
    pstmt.setLong(2,ele.cmsId)
    pstmt.setLong(3,ele.times)
    pstmt.addBatch()
   }
   pstmt.executeBatch()
   connection.commit()


  }
  catch {
   case e:Exception=>e.printStackTrace()
  }

 }

 def insertDayCityAccessTopN(list:ListBuffer[DayCItyAccessTopNStat]):Unit={
  var connection:Connection=null
  var pstmt:PreparedStatement=null

  try{
   connection=MySQLUtils.getConnection()
   connection.setAutoCommit(false)
   val sql="insert into day_video_city_access_topn_stat (day,city,cms_id,times,timesRank) values(?,?,?,?,?)"
   pstmt=connection.prepareStatement(sql)
   for (ele <- list){
    pstmt.setString(1,ele.day)
    pstmt.setString(2,ele.city)
    pstmt.setLong(3,ele.cmsId)
    pstmt.setLong(4,ele.times)
    pstmt.setLong(5,ele.times_rank)
    pstmt.addBatch()
   }
   pstmt.executeBatch()
   connection.commit()

  }
  catch {
   case e:Exception=>e.printStackTrace()
  }

 }

 def deleteData(day:String):Unit={
       val tables=Array(
        "day_video_access_topn_stat",
        "day_video_city_access_topn_stat",
        "day_video_traffics_topn_stat"
       )
     var connection:Connection=null
     var pstmt:PreparedStatement=null
   try{
    connection=MySQLUtils.getConnection()
    for(table<- tables){
     val deleteSQL=s"delete from $table where day=?"
     pstmt=connection.prepareStatement(deleteSQL)
     pstmt.setString(1,day)
     pstmt.executeUpdate()

    }


   }
  catch {
   case e:Exception=>e.printStackTrace()
  }
  finally {

   MySQLUtils.release(connection,pstmt)
  }

 }

 def main(args: Array[String]): Unit = {
       val day="20161110"
        deleteData(day)

 }

}



























