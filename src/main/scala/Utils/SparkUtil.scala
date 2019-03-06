package Utils

import org.apache.spark.{SparkConf, SparkContext}

object SparkUtil {
  def createSparkContext(isLocal:Boolean,appName:String): SparkContext ={
    if(isLocal) {
      val conf = new SparkConf()
        .setAppName(appName)
        .setMaster("local[*]")
        .set("spark.streaming.blockInterval","1s")
      val sc = SparkContext.getOrCreate(conf)
      sc
    }else if(isLocal && appName.endsWith("_mode")){
      val conf = new SparkConf()
        .setAppName(appName)
        .setMaster("local[*]")
        .set("spark.sql.shuffle.partitions","10")

      val sc = SparkContext.getOrCreate(conf)
      sc
    }
    else{
      val conf = new SparkConf()
        .setAppName(appName)
      val sc = SparkContext.getOrCreate(conf)
      sc
    }
  }
}
