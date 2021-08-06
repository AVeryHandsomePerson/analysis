package com.cn.bju.realtime.etl.process

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.cn.bju.common.bean.CanalRowData
import com.cn.bju.realtime.etl.`trait`.MysqlBaseETL
import com.cn.bju.realtime.etl.bean.RefundDetailDBEntity
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * 同步退款明细表
 *
 * @author ljh
 * @version 1.0
 */
class RefundDetailDataETL(env: StreamExecutionEnvironment) extends MysqlBaseETL(env) {

  /**
   * 根据业务抽取出来process方法，因为所有的ETL都有操作方法
   */
  override def process(): Unit = {
    //1：从kafka中消费出来订单数据，过滤出来订单表的数据
    val refundDataStream: DataStream[CanalRowData] = getKafkaDataStream()
      .filter(x => (x.getTableName == "refund_detail"))
//    // 将RowData转换成refundDBEntity对象
    val refundDBDataStream: DataStream[RefundDetailDBEntity] = refundDataStream.filter(rowData =>
      StringUtils.isNotEmpty(rowData.getColumns.get("create_time"))
    ).map(RefundDetailDBEntity(_))
    val refundDBJsonDataStream = refundDBDataStream.map(refundDB => {
      JSON.toJSONString(refundDB, SerializerFeature.DisableCircularReferenceDetect)
    })
    refundDBJsonDataStream.print("退款明细数据>>>")
    refundDBJsonDataStream.addSink(kafkaProducer("dwd_refund_detail"))


  }
}
