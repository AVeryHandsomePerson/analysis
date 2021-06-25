package Interceptor;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MultInterceptor implements Interceptor {
    private static final Logger logger = LoggerFactory.getLogger(MultInterceptor.class);
    // 过滤正则
    private static Pattern regex = null;
    // 截取标志
    private static Boolean cutFlag = true;
    // 总截取最大长度
    private static Integer cutMax = null;
    // 单个截取最大长度
    private static Integer singleCut = null;
    // 最后一个事件流
    private static List<Event> lastList = Lists.newArrayList();

    @Override
    public void initialize() {

    }

    //方法执行，处理单个event
    @Override
    public Event intercept(Event event) {

        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        List<Event> stringStream = list.stream().filter(x -> new String(x.getBody(), Charsets.UTF_8).contains("bju_pick:"))
                .map(x -> {
                    logger.info("目前数据是：" + new String(x.getBody(), Charsets.UTF_8));
                    x.setBody(new String(x.getBody(), Charsets.UTF_8).split("bju_pick:")[1].getBytes());
                    return x;
                }).collect(Collectors.toList());
        return stringStream;
    }

    @Override
    public void close() {
        logger.info("----------自定义拦截器close方法执行");
    }


    public static class Builder implements Interceptor.Builder {
        @Override
        public Interceptor build() {
            logger.info("----------build方法执行");
            return new MultInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
