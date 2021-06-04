import org.apache.spark.sql.SparkSession

object OdsAnalysis {
  def main(args: Array[String]): Unit = {
    //定义解析文件方式

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Ods_Analysis")


  }
}
