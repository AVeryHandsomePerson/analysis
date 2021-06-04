#! /bin/bash
dt=$(date -d '-1 day' '+%Y%m%d')
sh importHive.sh $dt
if [ $? -eq 0 ]; then
  echo '数据同步success'+$dt
else
  echo '数据同步fail'+$dt
  exit 1
fi
echo '执行拉链表'
/opt/cloudera/parcels/CDH-6.2.0-1.cdh6.2.0.p0.967373/lib/spark2/bin/spark-submit \
--master yarn \
--driver-memory 2g \
--executor-memory 3g \
--num-executors 3 \
--queue users \
--executor-cores 2 \
--jars /export/servers/bju/mysql-connector-java.jar \
--class common.ZipperTable /export/servers/bju/com.cn.bju.bigdata.offline.spark-1.0-SNAPSHOT.jar ${dt}
if [ $? -eq 0 ]; then
  echo '拉链表success'
else
  echo '拉链表fail'
  exit 1
fi

echo '执行中间表'
/opt/cloudera/parcels/CDH-6.2.0-1.cdh6.2.0.p0.967373/lib/spark2/bin/spark-submit \
--master yarn \
--driver-memory 2g \
--executor-memory 3g \
--num-executors 3 \
--queue users \
--executor-cores 2 \
--jars /export/servers/bju/mysql-connector-java.jar \
--class middle.OrdersMiddle /export/servers/bju/com.cn.bju.bigdata.offline.spark-1.0-SNAPSHOT.jar ${dt}
if [ $? -eq 0 ]; then
  echo success
else
  echo '执行中间表fail'
  exit 1
fi
echo '执行公共模块'
/opt/cloudera/parcels/CDH-6.2.0-1.cdh6.2.0.p0.967373/lib/spark2/bin/spark-submit \
--master yarn \
--driver-memory 2g \
--executor-memory 3g \
--num-executors 3 \
--queue users \
--executor-cores 2 \
--jars /export/servers/bju/mysql-connector-java.jar \
--class common.CommonAnalysis /export/servers/bju/com.cn.bju.bigdata.offline.spark-1.0-SNAPSHOT.jar ${dt}
if [ $? -eq 0 ]; then
  echo success
else
  echo '执行公共模块fail'
fi
/opt/cloudera/parcels/CDH-6.2.0-1.cdh6.2.0.p0.967373/lib/spark2/bin/spark-submit \
--master yarn \
--driver-memory 2g \
--executor-memory 3g \
--num-executors 3 \
--queue users \
--executor-cores 2 \
--jars /export/servers/bju/mysql-connector-java.jar \
--class shop.GoodsAnalysis /export/servers/bju/com.cn.bju.bigdata.offline.spark-1.0-SNAPSHOT.jar ${dt}
if [ $? -eq 0 ]; then
  echo '商品分析success'
else
  echo '商品分析fail'
fi

/opt/cloudera/parcels/CDH-6.2.0-1.cdh6.2.0.p0.967373/lib/spark2/bin/spark-submit \
--master yarn \
--driver-memory 2g \
--executor-memory 3g \
--num-executors 3 \
--queue users \
--executor-cores 2 \
--jars /export/servers/bju/mysql-connector-java.jar \
--class shop.OneGoodsAnalysis /export/servers/bju/com.cn.bju.bigdata.offline.spark-1.0-SNAPSHOT.jar ${dt}
if [ $? -eq 0 ]; then
  echo '单品分析success'
else
  echo '单品分析fail'
fi
/opt/cloudera/parcels/CDH-6.2.0-1.cdh6.2.0.p0.967373/lib/spark2/bin/spark-submit \
--master yarn \
--driver-memory 2g \
--executor-memory 3g \
--num-executors 3 \
--queue users \
--executor-cores 2 \
--jars /export/servers/bju/mysql-connector-java.jar \
--class shop.DealAnlaysis /export/servers/bju/com.cn.bju.bigdata.offline.spark-1.0-SNAPSHOT.jar ${dt}
if [ $? -eq 0 ]; then
  echo '交易分析success'
else
  echo '交易分析fail'
fi

/opt/cloudera/parcels/CDH-6.2.0-1.cdh6.2.0.p0.967373/lib/spark2/bin/spark-submit \
--master yarn \
--driver-memory 2g \
--executor-memory 3g \
--num-executors 3 \
--queue users \
--executor-cores 2 \
--jars /export/servers/bju/mysql-connector-java.jar \
--class shop.ClintAnalysis /export/servers/bju/com.cn.bju.bigdata.offline.spark-1.0-SNAPSHOT.jar ${dt}
if [ $? -eq 0 ]; then
  echo '客户分析success'
else
  echo '客户分析fail'
fi

/opt/cloudera/parcels/CDH-6.2.0-1.cdh6.2.0.p0.967373/lib/spark2/bin/spark-submit \
--master yarn \
--driver-memory 2g \
--executor-memory 3g \
--num-executors 3 \
--queue users \
--executor-cores 2 \
--jars /export/servers/bju/mysql-connector-java.jar \
--class shop.WarehouseAnalysis /export/servers/bju/com.cn.bju.bigdata.offline.spark-1.0-SNAPSHOT.jar ${dt}
if [ $? -eq 0 ]; then
  echo '仓库分析success'
else
  echo '仓库分析fail'
fi