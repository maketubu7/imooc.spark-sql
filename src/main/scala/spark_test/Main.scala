package spark_test

import scala.collection.JavaConversions._

import org.apache.spark.{SparkConf, SparkContext}

object Main {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Test").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val accum = new LogAccumulator
    sc.register(accum, "logAccum")
    val sum = sc.parallelize(Array("1", "2a", "3", "4b", "5", "6", "7cd", "8", "9"), 4).filter(line => {
      val pattern = """^-?(\d+)"""
      val flag = line.matches(pattern)
      if (!flag) {
        accum.add(line)
      }
      flag
    }).map(_.toInt).reduce(_ + _)

    println("sum: " + sum)
    for (v <- accum.value) print(v + " ")
    println()
    sc.stop()
  }
}

