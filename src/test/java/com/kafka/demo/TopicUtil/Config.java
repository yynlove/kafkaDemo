package com.kafka.demo.TopicUtil;

/**
 * 静态配置类
 */
public final class Config {

    /**
     * 连接ZK
     */
    private static final String ZK_CONNECT = "127.0.0.1:9092";
    /**
     * session 过期时间
     */
    private static final int SESSION_TIMEOUT = 30000;
    /**
     * 连接超时时间
     */
    private static final int CONNECT_TIMEOUT = 30000;
}
