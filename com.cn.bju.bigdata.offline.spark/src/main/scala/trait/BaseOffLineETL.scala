package `trait`

import org.apache.spark.sql.DataFrame

/**
 * @author ljh
 * @version 1.0
 */
trait BaseOffLineETL[T] {
  def writerClickHouse(df :DataFrame ,tableName :String)
  def process()
}
