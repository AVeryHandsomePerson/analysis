package com.cn.bju.realtime.etl.process

import com.alibaba.fastjson.{JSON, JSONObject}
import com.alibaba.fastjson.serializer.SerializerFeature
import com.cn.bju.realtime.etl.`trait`.MQBaseETL
import com.cn.bju.realtime.etl.bean.ClickLogEntity
import com.cn.bju.realtime.etl.util.GlobalConfigUtil
import ip.IPSeeker
import nl.basjes.parse.httpdlog.HttpdLoglineParser
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}

import java.io.File

/**
 *  * 点击流日志的实时ETL操作
 * 需要将点击流日志对象转换成拓宽后的点击流对象，增加省份、城市、时间字段
 * @author ljh
 * @version 1.0
 */
class ClickLogDataETL(env: StreamExecutionEnvironment) extends MQBaseETL(env) {
  /**
   * 根据业务抽取出来process方法，因为所有的ETL都有操作方法
   */
  override def process(): Unit = {
    /**
     * 实现步骤：
     * 1：获取点击流日志的数据源
     * 2：将nginx的点击流日志字符串转换成点击流对象
     * 3：对点击流对象进行实时拉宽操作，返回拉宽后的点击流实体对象
     * 4：将拉宽后点点击流实体类转换成json字符串
     * 5：将json字符串写入到kafka集群，供Druid进行实时的摄取操作
     */
    //获取点击流日志的数据源
    val clickLogDataStream: DataStream[String] = getKafkaDataStream(GlobalConfigUtil.`input.topic.click_log`)
    //将nginx的点击流日志字符串转换成点击流对象
    val clickLogWideEntityDataStream: DataStream[ClickLogEntity] = etl(clickLogDataStream)
    //将拉宽后点点击流实体类转换成json字符串
    val clickLogJsonDataStream: DataStream[String] = clickLogWideEntityDataStream.map(log => {
    //将拉宽后的点击流对象样例类转换成json字符串
    JSON.toJSONString(log, SerializerFeature.DisableCircularReferenceDetect)
    })
    //打印测试
    clickLogJsonDataStream.print("点击流数据>>>")
    //将json字符串写入到kafka集群
    clickLogJsonDataStream.addSink(kafkaProducer(GlobalConfigUtil.`output.topic.clicklog`))
  }
  /**
   * 将点击流日志字符串转换成拉宽后的点击流对象
   *
   * @param clickLogDataStream
   */
  def etl(clickLogDataStream: DataStream[String]) = {
    //解析 日志 转换成ClickLogEntity对象
    val clickLogEntityDataStream = clickLogDataStream.filter(_.contains("bju_pick:")).map(x => {
      ClickLogEntity(x.split("bju_pick:")(1))
    })
    // 拉宽数据
    val clickLogWideDataStream = clickLogEntityDataStream.map(new RichMapFunction[ClickLogEntity, ClickLogEntity] {
      //定义ip获取省份城市的实例对象
      var ipSeeker: IPSeeker = _

      //初始化操作，读取分布式缓存文件
      override def open(parameters: Configuration): Unit = {
        //读取分布式缓存文件
        val dataFile: File = getRuntimeContext.getDistributedCache.getFile("ipMapping.dat")
        //初始化Ipseeker的实例
        ipSeeker = new IPSeeker(dataFile)
      }

      override def map(in: ClickLogEntity): ClickLogEntity = {
        //根据ip地址获取省份、城市信息
        val country: String = ipSeeker.getCountry(in.ip)
        //如河南省郑州市
        var areaArray: Array[String] = country.split("省")
        if (areaArray.size > 1) {
          //表示非直辖市
          in.province = areaArray(0) + "省"
          in.city = areaArray(1)
        } else {
          //表示直辖市
          //如北京市海淀区
          areaArray = country.split("市")
          if (areaArray.length > 1) {
            in.province = areaArray(0) + "市"
            in.city = areaArray(1)
          } else {
            in.province = areaArray(0)
            in.city = ""
          }
        }
        in
      }
    })
    clickLogWideDataStream
  }
}
