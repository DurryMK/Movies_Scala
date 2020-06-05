package OtherCase

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
 * 具体数据结构如下所示：
 * * User
 * * |-- name: string (nullable = true)
 * * |-- registeredTime: string (nullable = true)
 * * |-- userID: long (nullable = true)
 * * *
 * * Log
 * * |-- consumed: double (nullable = true)
 * * |-- logID: long (nullable = true)
 * * |-- time: string (nullable = true)
 * * |-- typed: long (nullable = true)
 * * |-- userID: long (nullable = true)
 *
 * 需求:      用 sql方式或是 DataFrame Api方式完成以下需求:
 *             a. 特定时间段内用户访问电商网站排名Top 5 个用户 ( 2020-01-01 至 2020-01-15 日 )
 *             b. 统计特定时间段内用户购买总金额排名Top 5 个用户   ( 2020-01-01 至 2020-01-15 日 )
 *             c. 统计特定时间段内用户访问次数增长排名Top 5个用户, 例如说这一周比上一周访问次数增长最快的5位用户
 *             d. 统计特定时间段里购买金额增长最多的Top5用户，例如说这一周比上一周访问次数增长最快的5位用户
 *             e. 统计注册之后前两周内访问最多的前10个人
 *             f. 统计注册之后前两周内购买总额最多的Top 10
 *
 **/
object test3 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val session = SparkSession
    .builder()
    .appName("test1")
    .master("local[*]")
    .getOrCreate()

  import session.implicits._

  // e. 统计注册之后前两周内访问最多的前10个人

  var res = session.read.textFile("src/main/data/logs.log")
    .map(line => {
      var filed = line.split(",")
      var logId = filed(0).split(":")(1)
      var userId = filed(1).split(":")(1)

      var time = filed(2).split(":")(1).split("\"")(1).split(" ")(0).split("-")
      var year = Integer.parseInt(time(0))
      var mon = Integer.parseInt(time(1))
      var day = Integer.parseInt(time(2))

      var clazz = filed(3).split(":")(1)
      var consumed = filed(4).split(":")(1).replace("}", "")
      (logId, userId, year, mon, day, clazz, consumed)
    })
    .toDF("logId", "userId", "year", "mon", "day", "type", "num")
    .createTempView("logs")

  var res2 = session.read.textFile("src/main/data/users.log")
    .map(line => {
      var filed = line.split(",")
      var id = filed(0).split(":")(1)
      var name = filed(1).split(":")(1).replace("\"", "")
      var time = filed(2).split(":")(1).split("\"")(1).split(" ")(0)
      (id, name, time)
    })
    .map(x=>{
      var id = x._1
      var time = x._3.split("-")
      var y = Integer.parseInt(time(0))
      var m = Integer.parseInt(time(1))
      var d = Integer.parseInt(time(2))
      var ey = y
      var em = m
      var ed = d+14
      if(ed>31){
        ed = ed-31
        em += 1
      }
      if(em>12){
        ey+= 1
      }
      var userDF = session.sql("select * from logs where userId="+id).filter(x=>{
        x.getAs("year").toString.toInt<=ey&& x.getAs("mon").toString.toInt<=em&& x.getAs("day").toString.toInt<=ed
      })
      (userDF.count(),id)
    })
    .toDF("num","id")
    .show()


}

