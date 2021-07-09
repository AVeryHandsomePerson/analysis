import org.apache.commons.lang3.time.DateUtils
import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime

object OdsAnalysis {
  def main(args: Array[String]): Unit = {
    //定义解析文件方式

    val startTime = new DateTime(DateUtils.parseDate("20210707", "yyyyMMdd")).dayOfMonth().withMinimumValue().toString("yyyyMMdd")
     println(startTime)
  }
}
