package com.hive

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**ETL目标:
  * 1 过滤掉少于9个字段的数据
  * 2 将第四个字段中的" & "两边去掉空格
  * 3 将相关视频字段间相隔的"\t"转换为以"&"相隔
  * @author 朱同学
  *         date 2019/10/28
  *         description HiveProject com.hive
  *         version 1.0
  */
object ETLUtils {
  def main(args: Array[String]): Unit = {
    //输入输出路径
    var in = args(0)
    var out = args(1)
//    var in = "in"     //本地测试
//    var out = "out"   //本地测试

    //初始化
    val conf = new SparkConf().setMaster("local[*]").setAppName("HIVE_ETL")
    val sc = new SparkContext(conf)
    val oRDD = sc.textFile(in,2)

    //1 过滤掉少于9个字段的数据
    val filterRDD = oRDD.map(_.split("\t")).filter(_.length >= 9)
    //2 将第四个字段中的" & "两边去掉空格
    val map1RDD = filterRDD.map(array => {
      array(3) = array(3).replaceAll(" & ", "&")
      array
    })
    //3 将相关视频字段间相隔的"\t"转换为以"&"相隔
    val map2RDD = map1RDD.map(array => {
      if (array.length > 10) {
        val finalStr = new StringBuilder()
        for (i <- 9 until array.length) {
          if (i != array.length - 1) {
            finalStr ++= array(i) + "&"
          } else {
            finalStr ++= array(i)
          }
        }
        val arrayBuffer = new ArrayBuffer[String]()
        for (i <- 0 until 9) {
          arrayBuffer.append(array(i))
        }
        arrayBuffer.append(finalStr.toString())
        arrayBuffer.mkString("\t")
      } else {
        array.mkString("\t")
      }
    })
    map2RDD.map(array=>{

    })
//    map2RDD.take(10).foreach(println)  //本地测试
    //保存
    map2RDD.saveAsTextFile(out)

    sc.stop()
  }
}
