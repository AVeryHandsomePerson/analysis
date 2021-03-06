package shop

import `trait`.BaseETL
import common.StarvConfig
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * @author ljh
 * @version 1.0
 */
abstract class WriteBase extends BaseETL[SparkSession]{
  override def writerMysql(df: DataFrame, tableName: String, flag: String): Unit = {
    if(flag.equals("day")){
      df.write
        .mode(SaveMode.Append)
        .option(JDBCOptions.JDBC_BATCH_INSERT_SIZE, 100000)
        .jdbc(StarvConfig.url, tableName, StarvConfig.properties)
    }else if(flag.equals("week")){
      df.write
        .mode(SaveMode.Append)
        .jdbc(StarvConfig.url, s"""${tableName}_week""", StarvConfig.properties)
    }else if(flag.equals("month")){
      df.write
        .mode(SaveMode.Append)
        .jdbc(StarvConfig.url, s"""${tableName}_month""", StarvConfig.properties)
    }
  }
}
