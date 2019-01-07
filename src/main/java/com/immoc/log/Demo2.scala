package com.immoc.log

object Demo2 {
  def main(args: Array[String]): Unit = {
    def isIntByRegex(s : String) = {
      val pattern = """^(d+)$""".r
      s match {
        case pattern(_*) => true
        case _ => false
      }
    }


    isIntByRegex("123")
  }


}
