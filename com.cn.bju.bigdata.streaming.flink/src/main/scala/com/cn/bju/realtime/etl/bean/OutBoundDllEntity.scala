package com.cn.bju.realtime.etl.bean

import scala.beans.BeanProperty

/**
 * @author ljh
 * @version 1.0
 */
case class OutBoundDllEntity(
                              @BeanProperty id: Long, // 退款ID
                              @BeanProperty types: Int, // 出库类型
                              @BeanProperty orderId: Long, // 订单ID
                              @BeanProperty shopId: String, // 店铺
                              @BeanProperty create_time: String
                            )

case class OutBoundBillDetail(
                               @BeanProperty orderDetailId: Long, // 退款明细中退款主表ID
                               @BeanProperty cid: Int, // 品牌ID
                               @BeanProperty brandId: Int, // 类目ID
                               @BeanProperty itemId: Long, // 商品ID
                               @BeanProperty skuId: Long, // SKU_ID
                               @BeanProperty orderNum: Double, // 订单数量
                               @BeanProperty price: Double, // 价钱
                               @BeanProperty create_time: String // 价钱
                             )

case class OutBoundBillResultEvent(
                                    @BeanProperty id: Long, // 退款ID
                                    @BeanProperty types: Int, // 出库类型
                                    @BeanProperty orderId: Long, // 订单ID
                                    @BeanProperty shopId: String, // 店铺
                                    @BeanProperty orderDetailId: Long, // 退款明细中退款主表ID
                                    @BeanProperty cid: Int, // 品牌ID
                                    @BeanProperty brandId: Int, // 类目ID
                                    @BeanProperty itemId: Long, // 商品ID
                                    @BeanProperty skuId: Long, // SKU_ID
                                    @BeanProperty orderNum: Double, // 订单数量
                                    @BeanProperty price: Double, // 价钱
                                    @BeanProperty createTime: String, // 价钱
                                    @BeanProperty hour: String, //小时
                                    @BeanProperty day: String //天
                                  )
