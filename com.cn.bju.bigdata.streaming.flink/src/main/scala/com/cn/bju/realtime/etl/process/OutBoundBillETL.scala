package com.cn.bju.realtime.etl.process

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.cn.bju.realtime.etl.`trait`.MysqlBaseETL
import com.cn.bju.realtime.etl.bean.{OutBoundBillDetail, OutBoundBillResultEvent, OutBoundDllEntity}
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.functions.JoinFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.commons.lang3.time.DateUtils
import org.apache.flink.table.shaded.org.joda.time.DateTime

import java.util.concurrent.TimeUnit

/**
 * @author ljh
 * @version 1.0
 */
class OutBoundBillETL(env: StreamExecutionEnvironment) extends MysqlBaseETL(env) {


  /**
   * 根据业务抽取出来process方法，因为所有的ETL都有操作方法
   */
  override def process(): Unit = {




//    //   获取出库主表信息
    val outboundDll: DataStream[OutBoundDllEntity] = getKafkaDataStream()
      .filter(x => (x.getTableName == "outbound_bill" && StringUtils.isNotEmpty(x.getColumns.get("id"))))
      .map(x => {
        OutBoundDllEntity(
          x.getColumns.get("id").toLong ,
          x.getColumns.get("type").toInt,
          x.getColumns.get("order_id").toLong,
          x.getColumns.get("shop_id"),
          x.getColumns.get("create_time")
        )
      })
//
//
//    //    获取出库明细信息
    val outboundDllDetail: DataStream[OutBoundBillDetail] = getKafkaDataStream()
      .filter(x => (x.getTableName == "outbound_bill_detail"))
      .map(x => {
        OutBoundBillDetail(
          x.getColumns.get("outbound_bill_id").toLong,
          if (StringUtils.isNotEmpty(x.getColumns.get("cid"))) x.getColumns.get("cid").toInt else 0,
          if (StringUtils.isNotEmpty(x.getColumns.get("brand_id"))) x.getColumns.get("brand_id").toInt else 0,
          if (StringUtils.isNotEmpty(x.getColumns.get("item_id"))) x.getColumns.get("item_id").toLong else 0,
          if (StringUtils.isNotEmpty(x.getColumns.get("sku_id"))) x.getColumns.get("sku_id").toLong else 0,
          if (StringUtils.isNotEmpty(x.getColumns.get("order_num"))) x.getColumns.get("order_num").toDouble else 0.0,
          if (StringUtils.isNotEmpty(x.getColumns.get("price"))) x.getColumns.get("price").toDouble else 0.0,
          x.getColumns.get("create_time")
        )
      })
    //    对出库主表信息进行降维和分组
    val outboundDllJsonStream = outboundDll
      .assignAscendingTimestamps(x => {
        DateUtils.parseDate(x.create_time, "yyyy-MM-dd HH:mm:ss").getTime
      })
      .keyBy(_.id)
    //    对出库明细表进行降维和分组
    val outboundDllDetailJsonStream = outboundDllDetail.assignAscendingTimestamps(x => {
      DateUtils.parseDate(x.create_time, "yyyy-MM-dd HH:mm:ss").getTime
    }).keyBy(_.orderDetailId)
    //  以系统时间为间隔 没1秒处理一次数据, 出库明细表关联出库表信息 转换为OutBoundBillResultEvent对象输出
    val dataStream: DataStream[OutBoundBillResultEvent] = outboundDllDetailJsonStream.join(outboundDllJsonStream).where(_.orderDetailId).equalTo(_.id)
      .window(TumblingProcessingTimeWindows.of(Time.of(1, TimeUnit.SECONDS)))
      .apply(new InnerWindowJoinFunction)

    //将OutBoundBillResultEvent对象转换成Json字符串
    val outboundJsonDataStream: DataStream[String] = dataStream.map(orderDBEntity => {
      //将样例类转换成json字符串
      JSON.toJSONString(orderDBEntity, SerializerFeature.DisableCircularReferenceDetect)
    })
    outboundJsonDataStream.print("出库信息=========>")
    outboundJsonDataStream.addSink(kafkaProducer("dwd_outbound_bill_detail"))

  }
}


class InnerWindowJoinFunction extends JoinFunction[OutBoundBillDetail, OutBoundDllEntity, OutBoundBillResultEvent] {
  override def join(outBoundBillDetail: OutBoundBillDetail, outBoundDllEntity: OutBoundDllEntity): OutBoundBillResultEvent = {
    val dataTime = new DateTime(DateUtils.parseDate(outBoundBillDetail.getCreate_time, "yyyy-MM-dd HH:mm:ss"))
    OutBoundBillResultEvent(
      outBoundDllEntity.getId,
      outBoundDllEntity.getTypes,
      outBoundDllEntity.getOrderId,
      outBoundDllEntity.getShopId,
      outBoundBillDetail.getOrderDetailId,
      outBoundBillDetail.getCid,
      outBoundBillDetail.getBrandId,
      outBoundBillDetail.getItemId,
      outBoundBillDetail.getSkuId,
      outBoundBillDetail.getOrderNum,
      outBoundBillDetail.getPrice,
      outBoundBillDetail.create_time,
      dataTime.toString("HH"),
      dataTime.toString("yyyy-MM-dd")
    )
  }
}
