package com.kafka.demo.avro;

import com.kafka.demo.mySelf.StockPartitioner;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.text.DecimalFormat;
import java.util.Properties;
import java.util.Random;

public class AvroQuotationProducer {

    private static final int MSG_SIZE =100;
    private static final String TOPIC = "stock-quotation";
    private static final String BROKER_LIST = "localhost:9092";

    private static KafkaProducer<String,AvroStockQuotation> producer = null;
    static {
        Properties config = initConfig();
        producer =  new KafkaProducer<String,AvroStockQuotation>(config);
    }

    private static Properties initConfig() {
        final Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,BROKER_LIST);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, AvroSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        //引入自定义分区器
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, StockPartitioner.class);
        return properties;
    }


    private static AvroStockQuotation createQuotationInfo(){
        Integer stockCode = 60100+ new Random().nextInt(10);
        float random = (float) Math.random();
        if(random /2 <0.5){
            random = -random;
        }
        final DecimalFormat decimalFormat = new DecimalFormat(".00");
        final AvroStockQuotation build = AvroStockQuotation.newBuilder().build();
        build.setCurrentPrice(Float.valueOf(decimalFormat.format(11+random)));
        build.setPreClosePrice(11.80f);
        build.setOpenPrice(11.5f);
        build.setLowPrice(10.5f);
        build.setHighPrice(12.5f);
        build.setStockCode(stockCode.toString());
        build.setTradeTime(System.currentTimeMillis());
        build.setStockName("股票-"+stockCode);
        return build;
    }

    public  static void seedMsg(TopicEnum topic,AvroStockQuotation message){
        if(null == message){
            return;
        }
        if(StringUtils.equals(topic.getDataType().getClass().getName(),message.getClass().getName())){
            final ProducerRecord<String, AvroStockQuotation> stringAvroStockQuotationProducerRecord = new ProducerRecord<>(topic.getTopicName(), (String) message.getStockCode(), message);
            producer.send(stringAvroStockQuotationProducerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if(null != exception){
                        System.out.println("发送异常");
                    }
                    if(null != metadata){
                        System.out.println("发送成功>>>>>offset:"+metadata.offset()+"partation:"+metadata.partition());
                    }
                }
            });
        }
    }



    @Test
    public void seedAsyncTest(){
        AvroStockQuotation avroStockQuotation = null;
        try {
            int num =0;
            for (int i=0;i<MSG_SIZE ;i++){
                avroStockQuotation = createQuotationInfo();
                AvroQuotationProducer.seedMsg(TopicEnum.STOCK_QUOTATION_AVRO,avroStockQuotation);
                if(num++ % 10 == 0){
                    Thread.sleep(2000L);
                }
            }
        }catch (InterruptedException e){
            System.out.println(e.getMessage());
        }finally {
            producer.close();
        }
    }




}
