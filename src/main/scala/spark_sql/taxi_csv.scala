package spark_sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object taxi_csv {
  def main(args: Array[String]): Unit = {
    val csv_path = "datas/taxi.csv"

    val conf = new SparkConf()
      .setAppName("csv")
      .setMaster("local[*]")
      .set("spark.sql.shuffle.partitions","10")
    val sc = SparkContext.getOrCreate(conf)

    val sk = SparkSession.builder()
      .master("local[*]")
      .appName("csv")
      .getOrCreate()



    val schema = StructType(Array(
      StructField("tid",LongType),
      StructField("lan",StringType),
      StructField("len",StringType),
      StructField("time",StringType)
    ))
    sk.sqlContext.read
      .format("csv")
      .option("header",false)
      .schema(schema)
      .load(csv_path)
        .createOrReplaceTempView("taxi")


    sk.sql(
      """
        |SELECT tid,SUBSTR(time,0,2) AS hour
        |FROM taxi
      """.stripMargin).createOrReplaceTempView("taxi_hour")

    sk.sql(
      """
        |SELECT tid,hour,count(1) AS count
        |FROM taxi_hour
        |GROUP BY tid,hour
      """.stripMargin).createOrReplaceTempView("taxi_hour_count")


    sk.sql(
      """
        |SELECT tid,hour,count,
        |ROW_NUMBER() OVER(PARTITION BY hour ORDER BY count DESC) AS rnk
        |from taxi_hour_count
      """.stripMargin).createOrReplaceTempView("taxi_hour_count_rnk")

//    val res =  sk.sql(
//      """
//        |SELECT tid,hour,count,rnk
//        |FROM taxi_hour_count_rnk
//        |WHERE rnk <= 5
//      """.stripMargin)
    val res =  sk.sql(
      """
        |SELECT tid,hour,count,rnk
        |FROM taxi_hour_count_rnk
        |WHERE rnk < 5
      """.stripMargin)

    res.coalesce(3).write
      .format("csv")
      .option("header",true)
      //若不注释，则最后都会为12个分区文件,无论前面是怎么分区的
      .partitionBy("hour")
      .mode(SaveMode.Overwrite)
      .save("datas/res_taxi")

    Thread.sleep(200000)

  }

}
