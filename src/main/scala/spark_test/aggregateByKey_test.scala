package spark_test

object aggregateByKey_test {
  def main(args: Array[String]): Unit = {
    val sc = SparkUtil.createSparkContext(true,"aggregateByKey_test")

    val pairRdd = sc.parallelize(List((1,3),(1,2),(1,3),(1,34),(1,4),(2,3)),2)

//     (2,3)
//     (1,37)
//    val res = pairRdd.aggregateByKey(1)(math.max(_ , _),  _ + _ ).collect

//    (2,35)
//    (1,70)
      val res = pairRdd.aggregateByKey(35)(math.max(_ , _),  _ + _ ).collect

    for (s1 <- res){
      println(s1)
    }
  }

}
