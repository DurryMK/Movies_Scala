package avg

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType, LongType, StructField, StructType}

object simpleAvg extends App {
  val session = SparkSession //2.x的SparkSession
    .builder()
    .appName("app3")
    .master("local[*]")
    .getOrCreate()
  val sql = session.sqlContext

  session.udf.register("MyMean",MyMean)

  session.range(1,11).createTempView("range")

  session.sql("select MyMean(id) mm,avg(id) avg from range").show()

  session.stop()
}

object MyMean extends UserDefinedAggregateFunction {

  //聚合函数的输入数据的结构
  override def inputSchema: StructType = {
    StructType(List(StructField("value", DoubleType)))
  }

  //运算时产生的中间结果的缓存区数据结构
  override def bufferSchema: StructType = {
    StructType(List(StructField("product", DoubleType),
      StructField("counts", LongType)))
  }

  //返回值的数据结构
  override def dataType: DataType = {
    DoubleType
  }

  //是否幂等
  override def deterministic: Boolean = true

  //初始值的缓冲区
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 1.0
    buffer(1) = 0L
  }

  //新数据传入时调用update
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getDouble(0)*input.getDouble(0)
    buffer(1) = buffer.getLong(1)+1
  }

  //合并聚合函数缓冲区
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getDouble(0)*buffer2.getDouble(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  override def evaluate(buffer: Row): Any = {
    math.pow(buffer.getDouble(0),1.toDouble/buffer.getLong(1))
  }
}
