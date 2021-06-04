package com.cn.bju.realtime.etl.util

import org.apache.commons.lang3.StringUtils

/**
 * @author ljh
 * @version 1.0
 */
object CommonUtils {
  def isNotNull(data:String):Long={
    var bigInt = 0L
    if(StringUtils.isNotEmpty(data)) {
      bigInt = data.toLong
    }
    bigInt
  }
}
