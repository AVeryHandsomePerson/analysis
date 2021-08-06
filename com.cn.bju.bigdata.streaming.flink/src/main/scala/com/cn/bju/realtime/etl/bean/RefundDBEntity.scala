package com.cn.bju.realtime.etl.bean

import com.cn.bju.common.bean.CanalRowData
import com.cn.bju.realtime.etl.util.CommonUtils
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.time.DateUtils
import org.apache.flink.table.shaded.org.joda.time.DateTime

import scala.beans.BeanProperty

/**
 * @author ljh
 * @version 1.0
 */
case class RefundDBEntity(

                           @BeanProperty id: Int, // 退款ID
                           @BeanProperty orderId: Long, // 订单ID
                           @BeanProperty shopId: String, // 订单ID
                           @BeanProperty orderStatus: Int, // 订单状态
                           @BeanProperty refundStatus: Int, //退货状态
                           @BeanProperty refundReason: String, // 退款退货原因
                           @BeanProperty questionDescription: String, //问题描述
                           @BeanProperty refundTotalMoney: Double, //可退的总金额
                           @BeanProperty applyRefundMoney: Double, //可退的总金额
                           @BeanProperty updateTime: Long, // 方便下游CLICKHOUSE 做去重操作
                           @BeanProperty hour: String, //小时
                           @BeanProperty day: String
                         )
object RefundDBEntity {
  def apply(rowData: CanalRowData): RefundDBEntity = {
    val dataTime = new DateTime(DateUtils.parseDate(rowData.getColumns.get("create_time"), "yyyy-MM-dd HH:mm:ss"))
    RefundDBEntity(
      rowData.getColumns.get("id").toInt,
      CommonUtils.isNotNull(rowData.getColumns.get("order_id")),
      rowData.getColumns.get("shop_id"),
      CommonUtils.isNotNull(rowData.getColumns.get("order_status")).toInt,
      CommonUtils.isNotNull(rowData.getColumns.get("refund_status")).toInt,
      rowData.getColumns.get("refund_reason"),
      rowData.getColumns.get("question_description"),
      if(StringUtils.isNotEmpty(rowData.getColumns.get("refund_total_money"))) rowData.getColumns.get("refund_total_money").toDouble else 0,
      if(StringUtils.isNotEmpty(rowData.getColumns.get("apply_refund_money"))) rowData.getColumns.get("apply_refund_money").toDouble else 0,
      dataTime.getMillis,
      dataTime.toString("HH"),
      dataTime.toString("yyyy-MM-dd")
    )
  }
}
