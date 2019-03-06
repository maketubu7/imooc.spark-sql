package spark_sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

case class pirates(name:String,age:Int,salary:Double,deptno:Int)
case class dept(deptno:Int,deptname:String)
/**
  * Created by lenvo on 2018/7/29.
  */
/**
  * sparkDSL语法
  * sparksql其实是为了受众面更广，sql维护成本比较低
  *
  * 1/为了更方便维护，我推荐sql
  * 2/但是sql的可操作性相对比较小
  *    （当遇到了数据倾斜的时候）大表join普通的表，广播变量，filter
  * 3/能写sql的，尽量用sql代替，如果有计算时间等的优化考虑，可以把部分变成dsl
  * 4/spark虽然快，但是对内存太敏感了，可能使用spark的时候，更多要考虑数据倾斜
  *
  */

object DSL_demo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("csv")
      .setMaster("local[*]")
      .set("spark.sql.shuffle.partitions","10")
    val sc = SparkContext.getOrCreate(conf)

    val sk = SparkSession.builder()
      .master("local[*]")
      .appName("DSL_DEMO")
     // .enableHiveSupport()
      .getOrCreate()


    sk.sqlContext.udf.register("define_lv",(no:Int) =>{
      no match {
        case 1 => "第一战力"
        case 2 => "第二战力"
        case 3 => "打酱油"
        case 4 => "宠物"
        case _ => "菜鸡"
      }
    })

    sk.sqlContext.udf.register("self_avg",AVG_UDAF)

    val rdd_pirates = sc.parallelize(Array(
      pirates("路飞",18,9999.99,1),
      pirates("索隆",23,5555.55,1),
      pirates("罗宾",33,4444.44,2),
      pirates("娜美",22,3333.33,3),
      pirates("山治",24,5555.55,1),
      pirates("乌索普",19,5555.55,2),
      pirates("布鲁克",100,4888.88,2),
      pirates("罗",25,6666.66,1),
      pirates("乔巴",13,1000.11,4)
    ))

    val rdd_dept: RDD[dept] = sc.parallelize(Array(
      dept(1,"主战力"),
      dept(2,"副战力"),
      dept(4,"打酱油")
    ))



    import sk.implicits._

    val df_pirates: DataFrame = rdd_pirates.toDF()
    val df_dept: DataFrame = rdd_dept.toDF()

    df_dept.cache()
    df_pirates.cache()

    import sk.implicits._
    println("=======================DSl============================")

    df_pirates.select("name","age","salary","deptno").show
    //自定义udf
    df_pirates.selectExpr("name","age","salary","define_lv(deptno) as lv").show

    df_pirates.select($"name".as("name1"),$"age".as("age1"),$"salary".as("salary1")
      ,$"deptno".as("deptno1")).show()

    println("======================where/filter==========================")


    df_pirates.where($"salary" > 4000 && $"deptno" === 2).show()

    df_pirates.where("salary > 4000").where("age  < 25").show()

    println("======================sort==========================")
    //局部排序，和全局排序
    //sort(X)   对x的升序排序
    df_pirates.sort("salary").selectExpr("name","age","salary","define_lv(deptno) as lv").show()
    //对赏金的降序，相同按年龄的升序
    df_pirates.sort($"salary".desc,$"age".asc).selectExpr("name","age","salary"
      ,"define_lv(deptno) as lv").show()

    println("======================order by==========================")

    df_pirates.repartition(5).orderBy($"salary".desc,$"age".asc).selectExpr("name","age","salary"
      ,"define_lv(deptno) as lv").show()
    println("======================sortwithpartition==========================")


    df_pirates.sortWithinPartitions($"salary".desc,$"age".asc).selectExpr("name","age","salary"
      ,"define_lv(deptno) as lv").show()
    df_pirates.sortWithinPartitions($"salary".desc,$"age".desc).selectExpr("name","age","salary"
      ,"define_lv(deptno) as lv").show()

    println("======================groupby==========================")

    df_pirates.groupBy("deptno").agg(
      "salary" -> "sum",
      "salary" -> "avg",
      //自定义方法调用
      "deptno" -> "define_lv"
    ).show()

  /*  df_pirates.groupBy("deptno").agg(
      sum("salary").as("sum"),
      avg("salary").as("avg")
    )*/

    println("======================limit==========================")

    df_pirates.limit(4).show()

    println("======================join==========================")

    /**
      * 左关联，右关联，内联，外联，左半关联
      */
    //这种方式会报错 deptno来自于哪张表？
    //PersonDF.join(deptDF,$"deptno"===$"deptno").show

    df_pirates.join(df_dept,"deptno").show()

    //如果出现字段名称一样，之后无法处理，那么可以事后取出具体的字段
    //或者事后，修改字段名称（可能要写的比较长）
    df_pirates.join(df_dept,df_pirates.col("deptno")===df_dept.col("deptno"))
      .select(df_pirates.col("deptno"),df_dept.col("deptname")).show

    //.toDF("列1新名称","列2新名称"),这样关联的字段就不是重名字段
    df_dept.toDF("d1","dname").join(df_pirates,$"deptno"===$"d1").show

    println("===================左右关联等======================")
    //左边有的都要有，如dept=3的为null 也得有 如娜美

    df_pirates.join(df_dept,Seq("deptNo"),"left").show
    //右边有的都要有，右边有左边没有的 直接放弃  如娜美
    df_pirates.join(df_dept,Seq("deptNo"),"right").show
    //笛卡尔乘积，两边都得有
    df_pirates.join(df_dept,Seq("deptNo"),"full").show
    //select * from df_dept where deptNo in (select deptNo from df_pirates)
    df_dept.join(df_pirates,Seq("deptNo"),"leftsemi").show


    println("======================case when==========================")


    /**
      *  select *,
      *  case when salary <= 2000
      *  then "底薪"
      *  when salary > 2000 and salary <=4000
      *  then "中等水平"
      *  else "高薪" as salarylevel
      *  end from df_pirates
      *
      *    //基于某些已知的字段，为每一条记录打上更细的标签
      */

    df_pirates.createOrReplaceTempView("p")

    //when 报错
     df_pirates.select(
      df_pirates.col("name"),df_pirates.col("salary"),
      when(df_pirates.col("salary") <= 4000,"小海贼")
        .when(df_pirates.col("salary") > 4000 && df_pirates.col("salary") < 6000,"一般海贼")
        .otherwise("大海贼")
    )

    //直接用sql写  如下
    sk.sql(
    """
        |select name,salary,
        |(case when salary <= 4000
        |then "小海贼"
        |when (salary > 4000 and salary < 6000)
        |then "一般海贼"
        |else "大海贼" end ) as level
        |from p
      """.stripMargin).show()
    println("======================窗口函数==========================")


    /**
      * 使用窗口函数，就必须使用hivecontext
      * 按照部门分组，组内按salary排序，求每个部门的前三(可能需要做row_number或者unionall)
      *
      * select * from
      * (select *,
      * row_number() over (partition by deptno order by salary desc) as rnk
      * from df_pirates) a
      * where rnk <=3
      *
      * select * from df_pirates where deptno = 1 order by salary desc limit 3
      * union all
      * select * from df_pirates where deptno = 2 order by salary desc limit 3
      * union all
      * select * from df_pirates where deptno = 3 order by salary desc limit 3
      */

    val w = Window.partitionBy("deptno").orderBy($"salary".desc,$"age".asc)

    df_pirates.select($"name",$"age",$"deptno",$"salary",
      row_number().over(w).as("rnk")
    //leq(num),按升序排序取前num个
    ).where($"rnk".leq(5)).show

    println("=========================union ALL============================")

    //优先以salary进行排序，相同对age进行排序，相当于一个比较器的概念
    //也可以实现 分组排序   先分组进行排序，再拼接起来
    df_pirates.select($"name",$"age",$"deptno",$"salary"
    ).where($"deptno" === 1).sort($"salary".desc,$"age".asc).limit(3)
      .union(df_pirates.select($"name",$"age",$"deptno",$"salary"
      ).where($"deptno" === 2).sort($"salary".desc,$"age".asc).limit(3))
      .union(df_pirates.select($"name",$"age",$"deptno",$"salary"
      ).where($"deptno" === 3).sort($"salary".desc,$"age".asc).limit(3)).show


    df_pirates.distinct().show()

    //df_pirates.select(col = )
    Thread.sleep(200000)


    df_pirates.unpersist()
    df_dept.unpersist()

  }
}
