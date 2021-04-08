package com.kafka.demo.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.CountDownLatch;
import java.util.logging.Logger;

public class KafkaProducerThread implements Runnable {

    private KafkaProducer<String,String> producer = null;

    private ProducerRecord<String,String> record = null;

    public KafkaProducerThread(KafkaProducer<String, String> producer, ProducerRecord<String, String> record) {
        this.producer = producer;
        this.record = record;
    }

    @Override
    public void run() {
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if(e != null){
                    //发送异常记录异常信息
                    System.out.println("发送异常------->>>>>>>"+e.getMessage());
                }
                if(recordMetadata != null){
                    System.out.println("发送成功------->>>>>>>"+"偏移量:"+recordMetadata.offset()+ "分区："+recordMetadata.partition());
                }
            }
        });
    }
}
