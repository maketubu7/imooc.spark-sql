package spark_sql

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

object AVG_UDAF extends UserDefinedAggregateFunction{
  override def inputSchema: StructType = {
    //用户输入参数的类型
    StructType(Array
    (StructField("inpuvalue",DoubleType)))
  }

  override def bufferSchema: StructType = {
    //缓冲区的值（用于储存上一次相加之后计算出来的值）
    StructType(Array(
      StructField("totalvalue",DoubleType),
      StructField("totalcount",IntegerType)
    ))

  }

  override def dataType: DataType = {
    //返回值类型
    DoubleType
  }

  override def deterministic: Boolean = {
    true
  }

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    //缓冲区的初始值
    buffer.update(0,0.0)
    buffer.update(1,0)
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    //拿出缓存的值，拿出新的一条记录的值，然后相加，更新缓存的值
    val iv = input.getDouble(0)

    val bv = buffer.getDouble(0)
    val bc = buffer.getInt(1)

    buffer.update(0,bv + iv)
    buffer.update(1,bc + 1)

  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    //对两个buffer  的值进行合并

    val bv1 = buffer1.getDouble(0)
    val bc1 = buffer1.getInt(1)

    val bv2 = buffer2.getDouble(0)
    val bc2 = buffer2.getInt(1)

    buffer1.update(0,bv1+bv2)
    buffer1.update(1,bc1+bc2)

  }

  override def evaluate(buffer: Row): Any = {
    //求值的方法定义

    val bv = buffer.getDouble(0)

    val bc = buffer.getInt(1)

    bv / bc
  }
}
