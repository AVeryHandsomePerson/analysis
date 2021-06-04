package com.cn.bju.pick;

/**
 * @author ljh
 * canal客户端的入口类
 * @version 1.0
 * @date 2021/4/15 18:34
 *
 */
public class Entrance {
    public static void main(String[] args) {
        //实例化canal的客户端对象，调用start方法拉取canalserver端binlog日志
        CanalClient canalClient = new CanalClient();
        canalClient.start();
    }
}
