package com.lilei.test

object App13 {

  def main(args: Array[String]): Unit = {

    val map = Map("id" -> 2,"is" -> true)

    val j = scala.util.parsing.json.JSONObject(map)

    println(j)
  }

}