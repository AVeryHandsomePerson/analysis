package shop

import `trait`.BaseOffLineETL
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.util.Properties

/**
 * @author ljh
 * @version 1.0
 */
abstract class WriteOffLineBase extends BaseOffLineETL[SparkSession] {
  override def writerClickHouse(df: DataFrame, tableName: String): Unit = {
    val url = "jdbc:clickhouse://10.30.0.240:8123/off_line_dws"
    val pro = new Properties()
    pro.put("driver", "ru.yandex.clickhouse.ClickHouseDriver")
    pro.put("password", "6lYaUiFi")
    val write_maps = Map[String, String](
      "batchsize" -> "2000",
      "isolationLevel" -> "NONE",
      "numPartitions" -> "1"
    )
    df.write.mode(SaveMode.Append)
      .options(write_maps)
      .jdbc(url, tableName, pro)
  }
}
