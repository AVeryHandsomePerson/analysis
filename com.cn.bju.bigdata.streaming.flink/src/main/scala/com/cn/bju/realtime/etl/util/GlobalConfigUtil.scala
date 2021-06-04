package com.cn.bju.realtime.etl.util

import com.typesafe.config.ConfigFactory


/**
 * @author ljh
 *         编写读取配置文件的工具类
 * @version 1.0
 */
object GlobalConfigUtil {
  //默认在resources目录下找application名字的配置文件
  private val config = ConfigFactory.load()
  /**
   * 读取配置文件的配置项信息
   */
  val `bootstrap.servers` = config.getString("bootstrap.servers")
  val `zookeeper.connect` = config.getString("zookeeper.connect")
  val `input.topic.canal` = config.getString("input.topic.canal")
  val `input.topic.click_log` = config.getString("input.topic.click_log")
  val `input.topic.comments` = config.getString("input.topic.comments")
  val `group.id` = config.getString("group.id")
  val `enable.auto.commit` = config.getString("enable.auto.commit")
  val `auto.commit.interval.ms` = config.getString("auto.commit.interval.ms")
  val `auto.offset.reset` = config.getString("auto.offset.reset")
  val `key.serializer` = config.getString("key.serializer")
  val `key.deserializer` = config.getString("key.deserializer")
  val `input.topic.cart` = config.getString("input.topic.cart")

  val `output.topic.clicklog` = config.getString("output.topic.click_log")


}
