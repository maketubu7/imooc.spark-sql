package spark_test

import org.apache.spark.{SparkConf, SparkContext}

object SparkUtil {
  def createSparkContext(isLocal:Boolean,appName:String): SparkContext ={
    if(isLocal) {
      val conf = new SparkConf()
        .setAppName(appName)
        .setMaster("local[*]")
      val sc = SparkContext.getOrCreate(conf)
      sc
    }else{
      val conf = new SparkConf()
        .setAppName(appName)

      val sc = SparkContext.getOrCreate(conf)
      sc

    }
  }
}
