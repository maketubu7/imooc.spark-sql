package spark_test

object partitions_test {
  def main(args: Array[String]): Unit = {
    val sc = SparkUtil.createSparkContext(true,"partitions_test")

    //两个分区
    val data = Array((3, 6),(5,4),(6, 2)) //key升序  自然顺序排序
    val rdd1 = sc.parallelize(data) //设置2分区
    var sum = 0
    val s = rdd1.glom.collect
    for(s1 <- s){
      println(s1)
    }

    rdd1.foreach(f => {
      println((f._1,sum+f._2) + "--" + sum)
      sum += f._2
    })
  }

}
