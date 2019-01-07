package project.domain

/**
  *
  * @param ip 日志访问的IP地址
  * @param time 日志访问的时间
  * @param courseId
  * @param statusCode
  * @param referer
  */


case class ClickLog(ip:String,time:String,courseId:Int,statusCode:Int,referer:String)
