package com.cn.bju.realtime.etl.process

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.cn.bju.common.bean.CanalRowData
import com.cn.bju.realtime.etl.`trait`.MysqlBaseETL
import com.cn.bju.realtime.etl.async.AsyncOrderDetailRedisRequest

import scala.concurrent.Future
//import com.cn.bju.realtime.etl.async.{AsyncOrderDetailRedisRequest, AsyncOrderDetailRedisRequests}
import com.cn.bju.realtime.etl.bean.OrderGoodsWideEntity
import org.apache.flink.streaming.api.scala.AsyncDataStream
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.async.{ResultFuture, RichAsyncFunction}

import java.util.Collections
import java.util.concurrent.TimeUnit

/**
 * @author ljh
 * @version 1.0
 */
class OrderDetailDataETL(env: StreamExecutionEnvironment) extends MysqlBaseETL(env){



  /**
   * 根据业务抽取出来process方法，因为所有的ETL都有操作方法
   */
  override def process(): Unit = {

    //1：从kafka中消费出来订单数据，过滤出来订单表的数据
    val orderDataStream: DataStream[CanalRowData] = getKafkaDataStream()
      .filter(_.getTableName == "orders_detail")
    // 从redis 中映射 需要的字段维度 目前只需要映射出 订单所在的店铺ID
    val orderGoodsWideEntityDataStream: DataStream[OrderGoodsWideEntity]= AsyncDataStream.unorderedWait(orderDataStream, new AsyncOrderDetailRedisRequest(),
      100, TimeUnit.SECONDS, 100)
//    将拉宽后的订单明细表的数据转换成json字符串写入到kafka集群
    val orderGoodsWideJsonDataStream: DataStream[String] = orderGoodsWideEntityDataStream.map(orderGoods => {
      JSON.toJSONString(orderGoods, SerializerFeature.DisableCircularReferenceDetect)
    })
    orderGoodsWideJsonDataStream.print("拉宽后的订单明细数据>>>")
    orderGoodsWideJsonDataStream.addSink(kafkaProducer("dwd_order_detail"))

  }
}
