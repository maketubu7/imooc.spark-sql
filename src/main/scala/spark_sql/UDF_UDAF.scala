package spark_sql

import org.apache.spark.sql.SparkSession

object UDF_UDAF {
  def main(args: Array[String]): Unit = {
    //spark有udf,usaf暂时还不支持udtf
    /**
      * 但是还是要关注   行转列   列转行
      */
    val sc = SparkUtil.createSparkContext(true,"rdd2dataframe")

    val sk = SparkSession.builder()
      .master("local[*]")
      .appName("rdd2dataframe")
      .getOrCreate()

    import sk.implicits._
    sk.sqlContext.udf.register("format_double",(value:Double) =>{
      import java.math.BigDecimal;

      val res = new BigDecimal(value)

      res.setScale(2,BigDecimal.ROUND_HALF_UP).doubleValue()
    })

    sk.sqlContext.udf.register("format_double_define",(value:Double,num:Int) =>{
      import java.math.BigDecimal;

      val res = new BigDecimal(value)

      res.setScale(num,BigDecimal.ROUND_HALF_UP).doubleValue()
    })

    sk.sqlContext.udf.register("self_avg",AVG_UDAF)

    sc.parallelize(Array(
      (1,6528),
      (1,4850),
      (1,3800),
      (1,3543),
      (1,3823),
      (1,3867),
      (2,7200),
      (2,3477),
      (2,7777),
      (2,3323),
      (2,7439),
      (2,3342)
    )).toDF("dept","salary").createOrReplaceTempView("temp")


    sk.sql(
      """
        |select dept,avg(salary),
        |self_avg(salary) as self_avg,
        |format_double(avg(salary)) double_avg,
        |format_double_define(avg(salary),4) as num_avg
        |from temp
        |group by dept
      """.stripMargin).show(2,100)

    sk.sql(
      """
        |select dept,concat_ws(",",collect_set(salary))
        |from temp a
        |group by a.dept
      """.stripMargin).show()

  }

}
