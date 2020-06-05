package OtherCase

import com.alibaba.fastjson.JSON
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
 *             c. 统计注册之后前两周内访问最多的前10个人
 *             d. 统计注册之后前两周内购买总额最多的Top 10
 *
 * */
object test1 extends  App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val session = SparkSession
    .builder()
    .appName("test1")
    .master("local[*]")
    .getOrCreate()

  import session.implicits._

  // a. 特定时间段内用户访问电商网站排名Top 5 个用户 ( 2020-01-01 至 2020-01-15 日 )

  var res = session.read.textFile("src/main/data/logs.log")
    .map(line=>{
     var filed =  line.split(",")
      var logId = filed(0).split(":")(1)
      var userId = filed(1).split(":")(1)

      var time = filed(2).split(":")(1).split("\"")(1).split(" ")(0).split("-")
      var year = Integer.parseInt(time(0))
      var mon = Integer.parseInt(time(1))
      var day = Integer.parseInt(time(2))

      var clazz = filed(3).split(":")(1)
      var consumed = filed(4).split(":")(1).replace("}","")
      (logId,userId,year,mon,day,clazz,consumed)
    })
    .toDF("logId","userId","year","mon","day","type","num")
    .createTempView("log")

  session.sql("select count(userId) num,userId from log where year=2020 and mon=1 and day<=15 group by userId order by num desc").show()


}
