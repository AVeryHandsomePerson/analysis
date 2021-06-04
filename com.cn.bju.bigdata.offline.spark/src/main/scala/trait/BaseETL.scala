package `trait`

import org.apache.spark.sql.DataFrame

/**
 * @author ljh
 * @version 1.0
 */
trait BaseETL[T] {

  def writerMysql(df :DataFrame ,tableName :String,flag :String)
  def process()
}
