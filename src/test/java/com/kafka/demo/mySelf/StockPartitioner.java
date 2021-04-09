package com.kafka.demo.mySelf;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * 自定义分区器
 */
public class StockPartitioner implements Partitioner {

    private static final Integer PARTTITIONERS = 6;


    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        if (null == key){
            return 0;
        }
        String stockCodes =String.valueOf(key);
        try {
            final int i = Integer.valueOf(stockCodes.substring(stockCodes.length() - 2)) % PARTTITIONERS;
            return i;
        }catch (NumberFormatException e){
            System.out.println("数字转化异常");
            return 0;
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
