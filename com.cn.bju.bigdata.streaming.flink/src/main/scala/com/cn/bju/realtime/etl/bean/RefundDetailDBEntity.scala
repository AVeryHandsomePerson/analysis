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
case class RefundDetailDBEntity(

                           @BeanProperty id: Int, // 退款明细ID
                           @BeanProperty refundId: Long, // 退款ID
                           @BeanProperty itemId: Long, // 退款ID
                           @BeanProperty itemName: String, // 退款ID
                           @BeanProperty skuId: Long,
                           @BeanProperty skuPicUrl: String, // 商品图片
                           @BeanProperty num: Double, // 总购买数量
                           @BeanProperty refundNum: Double, // 申请退款数量
                           @BeanProperty refundPrice: Double, // 退款单价
                           @BeanProperty hour: String, //小时
                           @BeanProperty day: String
                         )


object RefundDetailDBEntity{
  def apply(rowData: CanalRowData): RefundDetailDBEntity = {
  val dataTime = new DateTime(DateUtils.parseDate(rowData.getColumns.get("create_time"), "yyyy-MM-dd HH:mm:ss"))
  RefundDetailDBEntity(
  rowData.getColumns.get("id").toInt,
  CommonUtils.isNotNull(rowData.getColumns.get("refund_id")),
  rowData.getColumns.get("item_id").toInt,
  rowData.getColumns.get("item_name"),
  CommonUtils.isNotNull(rowData.getColumns.get("sku_id")),
  rowData.getColumns.get("sku_pic_url"),
  if(StringUtils.isNotEmpty(rowData.getColumns.get("num"))) rowData.getColumns.get("num").toDouble else 0,
  if(StringUtils.isNotEmpty(rowData.getColumns.get("refund_num"))) rowData.getColumns.get("refund_num").toDouble else 0,
  if(StringUtils.isNotEmpty(rowData.getColumns.get("refund_price"))) rowData.getColumns.get("refund_price").toDouble else 0,
  dataTime.toString("HH"),
  dataTime.toString("yyyy-MM-dd")
  )
}
}