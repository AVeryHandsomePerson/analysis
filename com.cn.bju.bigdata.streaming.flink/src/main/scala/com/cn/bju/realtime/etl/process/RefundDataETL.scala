package com.cn.bju.realtime.etl.process

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.cn.bju.common.bean.CanalRowData
import com.cn.bju.realtime.etl.`trait`.MysqlBaseETL
import com.cn.bju.realtime.etl.bean.RefundDBEntity
import org.apache.commons.lang3.StringUtils
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

/**
 * 同步退款主表 方便后续计算退款金额
 * 退款目前需要维度：
 * 退款ID，订单ID,退款时间, 退款金钱
 *
 * @author ljh
 * @version 1.0
 */
class RefundDataETL(env: StreamExecutionEnvironment) extends MysqlBaseETL(env) {

  /**
   * 根据业务抽取出来process方法，因为所有的ETL都有操作方法
   */
  override def process(): Unit = {
    //1：从kafka中消费出来订单数据，过滤出来订单表的数据
    val refundDataStream: DataStream[CanalRowData] = getKafkaDataStream()
      .filter(x => (x.getTableName == "refund_apply"))
    // 将RowData转换成refundDBEntity对象
    val refundDBDataStream: DataStream[RefundDBEntity] = refundDataStream.filter(rowData =>
      StringUtils.isNotEmpty(rowData.getColumns.get("create_time"))
    ).map(RefundDBEntity(_))
    val refundDBJsonDataStream = refundDBDataStream.map(refundDB => {
      JSON.toJSONString(refundDB, SerializerFeature.DisableCircularReferenceDetect)
    })
    refundDBJsonDataStream.print("退款数据>>>")
    refundDBJsonDataStream.addSink(kafkaProducer("dwd_refund_apply"))
  }
}
