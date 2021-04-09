package com.kafka.demo.avro;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class AvroQuotationConsumer {


    private static KafkaConsumer<String,AvroStockQuotation> consumer = null;

    private static final long TIME_OUT = 30L;

    static {

        final Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"avro-consumer");
        //设置手动提交偏移量 -- false
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,true);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,AvroDeseriazlizer.class.getName());
        consumer = new KafkaConsumer<>(properties);
    }

    public void consumer(String topicName){
        try {
          while (true){
              consumer.subscribe(Collections.singletonList(topicName));
              final ConsumerRecords<String, AvroStockQuotation> poll = consumer.poll(Duration.ofMillis(3000));
              AvroStockQuotation quotation = null;
              if(null != poll){
                  for (ConsumerRecord<String,AvroStockQuotation> record:poll){
                      final AvroStockQuotation value = record.value();
                      System.out.println("成功消费消息>>>>>>>>>>>>>>>>>>>>>>>:"+value.getStockCode());
                  }
              }
          }
        }catch (Exception e){
            System.out.println("读取消息错误");
        }finally {
            consumer.close();
            consumer= null;
        }

    }


    @Test
    public  void feelConsumerTest() {
        final AvroQuotationConsumer avroQuotationConsumer = new AvroQuotationConsumer();
        while (true){
            avroQuotationConsumer.consumer(TopicEnum.STOCK_QUOTATION_AVRO.getTopicName());
        }
    }
}
