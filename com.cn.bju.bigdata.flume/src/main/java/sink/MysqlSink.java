package sink;

import Interceptor.MultInterceptor;
import client.MysqlConnect;
import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Throwables;
import configuration.MysqlSinkConfigurationConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author ljh
 * @version 1.0
 */
public class MysqlSink extends AbstractSink implements Configurable {


    private static final Logger logger = LoggerFactory.getLogger(MysqlSink.class);

    private JdbcTemplate template;
    private String url;
    private String userName;
    private String password;
    private String hostName;
    private String port;
    private String databaseName;
    private String driver;
    private String sql;
    private int batchSize;

    @Override
    public void configure(Context context) {
        hostName = context.getString(MysqlSinkConfigurationConstants.hostName);
        port = context.getString(MysqlSinkConfigurationConstants.port);
        databaseName = context.getString(MysqlSinkConfigurationConstants.databaseName);
        url = "jdbc:mysql://" + hostName + ":" + port + "/" + databaseName + "?useUnicode=true&characterEncoding=UTF-8";
        userName = context.getString(MysqlSinkConfigurationConstants.USER_NAME);//用户名
        password = context.getString(MysqlSinkConfigurationConstants.PASSWORD);//密码
        driver = context.getString(MysqlSinkConfigurationConstants.DRIVER);//密码
        batchSize = context.getInteger(MysqlSinkConfigurationConstants.BATCH_SIZE);
    }
    @Override
    public synchronized void start() {
        super.start();
        template = new JdbcTemplate(MysqlConnect.dataSource(driver, url, userName, password));
        sql = " INSERT INTO pick_event_status(event, title,field,create_time) VALUE(?,?,?,?) ON DUPLICATE KEY UPDATE field=? , update_time=? ";
    }
    @Override
    public Status process() throws EventDeliveryException {
        Status status = Status.READY;
        Channel channel = getChannel();
        Transaction txn = channel.getTransaction();
        List<Object[]> list = new ArrayList<Object[]>();
        txn.begin();
        int count;
        try {
            logger.info("-----------------------------开始处理数据");
            for (count = 0; count < batchSize; ++count) {
                Event event = channel.take();
                if (event == null) {
                    status = Status.BACKOFF;
                    break;
                }
                DateTimeFormatter fmTime = DateTimeFormatter.ofPattern("yyyy-MM-dd");
                //当前时间
                LocalDateTime now = LocalDateTime.now();
                String dataEvent = new String(event.getBody());
                JSONObject jsonObject = JSON.parseObject(dataEvent);
                //解析字段维度
                List<String> collect = jsonObject.entrySet().stream().map(x -> x.getKey()).collect(Collectors.toList());
                String fieldStrip = StringUtils.strip(collect.toString(), "[]");
                Object[] object  = new Object[]{
                        jsonObject.getString("event"),
                        jsonObject.getString("title"),
                        fieldStrip,
                        now.format(fmTime),
                        fieldStrip,
                        now.format(fmTime)
                };

                list.add(object);
            }
            if (list.size() > 0) {
                template.batchUpdate(sql,list);
            }
            txn.commit();
        } catch (Throwable e) {
            try {
                txn.rollback();
            } catch (Exception e2) {
                logger.error("Exception in rollback. Rollback might not have been" +
                        "successful.", e2);
            }
            logger.error("Failed to commit transaction." +
                    "Transaction rolled back.", e);
            Throwables.propagate(e);
        } finally {
            txn.close();
        }
        return status;
    }
    @Override
    public synchronized void stop() {
        super.stop();
        logger.info("---------------------------自定义sink");

    }


}
