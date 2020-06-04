import org.apache.spark.sql.{Row, SparkSession}

object Ip_DSL {
  def ip2Long(ip: String): Long = {
    val framents = ip.split("\\.")
    var ipNum = 0L
    for (i <- 0 until framents.length) {
      ipNum = framents(i).toLong | ipNum << 8L
    }
    ipNum
  }

  def binarySearch(ipRules: Array[Row], ip: Long): Int = {
    var low = 0
    var high = ipRules.length - 1
    while (low <= high) {
      var middle = (low + high) / 2
      if ((ip >= ipRules(middle).getAs("start").asInstanceOf[Long]  ) && (ip <= ipRules(middle).getAs("end").asInstanceOf[Long])) {
        return middle
      } else if (ip < ipRules(middle).getAs("start").asInstanceOf[Long]) {
        high = middle - 1
      } else {
        low = middle + 1
      }
    }
    -1
  }

  def main(args: Array[String]): Unit = {
    val session = SparkSession //2.x的SparkSession
      .builder()
      .appName("app3")
      .master("local[*]")
      .getOrCreate()

    var sql = session.sqlContext

    import session.implicits._
    var res = session.read.textFile("E://ip.txt").map(x => {
      var line = x.split("\\|")
      IpRule(line(2).toLong, line(3).toLong, line(6).toString)
    }).toDF("start", "end", "province").collect()

    var bv = session.sparkContext.broadcast(res)

    var res2 = session.read.textFile("E://access.log").map(line => {
      val fields = line.split("\\|")
      ip2Long(fields(1))
    }).toDF("ip")

    res2.createTempView("access")



    session.udf.register("ip2P", (ip: Long) => {
      var rule = bv.value
      var province = "unkown"
      var index = binarySearch(rule, ip)
      if(index != -1){
        province = rule(index).getAs("province")
      }
      province
    })

   var s =  res2.selectExpr($"ip2P(ip)".as("province").toString()).groupBy($"province")
    s.count().show()
  }
}
