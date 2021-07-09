import common.{ReadStarvConfig, StarvConfig}
import ip.IPSeeker
import org.apache.spark.SparkFiles
import org.apache.spark.sql.SparkSession

import java.io.{File, FileInputStream, InputStream, RandomAccessFile}
import java.util.Properties
import scala.io.{BufferedSource, Source}

/**
 * @author ljh
 * @version 1.0
 */
object a {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Anlysis")
      .master("local")
      .config("hive.exec.dynamici.partition", true)
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()
    spark.udf.register("ip_mapping", func = (ip: String) => {
      val file = new File("/export/servers/bju/bin/ipMapping.dat")
      val ipSeeker: IPSeeker = new IPSeeker(file)
      val country = ipSeeker.getCountry(ip)
      var province = ""
      var city = ""
      //如河南省郑州市
      var areaArray: Array[String] = country.split("省")
      if (areaArray.size > 1) {
        //表示非直辖市
        province = areaArray(0) + "省"
        city = areaArray(1)
      } else {
        //表示直辖市
        //如北京市海淀区
        areaArray = country.split("市")
        if (areaArray.length > 1) {
         province = areaArray(0) + "市"
         city = areaArray(1)
        } else {
          province = areaArray(0)
          city = ""
        }
      }
      province+","+city
    })
        spark.read.json("/click_log/20210705/*").createOrReplaceTempView("click_log")
        spark.sql(
          """
            |
            |select
            |ip,
            |split(ip_mapping(ip),',')[0] as province,
            |split(ip_mapping(ip),',')[1] as city
            |from
            |click_log
            |""".stripMargin).show()




  }
}
