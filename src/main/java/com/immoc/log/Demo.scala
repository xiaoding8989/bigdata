package com.immoc.log

object Demo {

  def strtoInt(str:String):String={
    val regex="""([0-9]+)""".r
    str match {
      case regex(str)=>str
      case _=>"0"
    }

  }

  def main(args: Array[String]): Unit = {
    println(strtoInt("a123456").toLong)

  }


}
