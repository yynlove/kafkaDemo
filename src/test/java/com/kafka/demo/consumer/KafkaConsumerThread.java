package com.kafka.demo.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class KafkaConsumerThread extends Thread {
    //每个线程私有的KafkaConsumer实例
    private KafkaConsumer<String,String> consumer;

    public KafkaConsumerThread(Properties properties,String topic){
        this.consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));
    }

    @Override
    public void run() {

        try{
            while (true){
                final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String,String> record :records){
                    System.out.println("消息>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>:partation:"+record.partition()+",offset:"+record.offset()+",key:"+record.key()+",value:"+record.value());
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            consumer.close();
        }



    }
}
