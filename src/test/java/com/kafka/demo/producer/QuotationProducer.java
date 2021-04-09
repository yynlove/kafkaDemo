package com.kafka.demo.producer;

import com.kafka.demo.bean.StockInfo;
import com.kafka.demo.mySelf.StockPartitioner;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.text.DecimalFormat;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

/**
 * 股票行情生产者
 */
public class QuotationProducer {


    private static  final  Logger log = Logger.getLogger("QuotationProducer");

    private static final int MSG_SIZE =100;
    private static final String TOPIC = "stock-quotation";
    private static final String BROKER_LIST = "localhost:9092";

    private static KafkaProducer<String,String> producer = null;
    static {
        Properties config = initConfig();
        producer =  new KafkaProducer<String,String>(config);
    }

    private static Properties initConfig() {
        final Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,BROKER_LIST);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        //引入自定义分区器
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, StockPartitioner.class);
        return properties;
    }


    private static StockInfo createQuotationInfo(){

        final StockInfo stockInfo = new StockInfo();
        Integer stockCode = 60100+ new Random().nextInt(10);
        float random = (float) Math.random();
        if(random /2 <0.5){
            random = -random;
        }

        final DecimalFormat decimalFormat = new DecimalFormat(".00");
        stockInfo.setCurrentPrice(Float.valueOf(decimalFormat.format(11+random)));
        stockInfo.setPreClosePrice(11.80f);
        stockInfo.setOpenPrice(11.5f);
        stockInfo.setLowPrice(10.5f);
        stockInfo.setHighPrice(12.5f);
        stockInfo.setStockCode(stockCode.toString());
        stockInfo.setTradeTime(System.currentTimeMillis());
        stockInfo.setStockName("股票-"+stockCode);
        return stockInfo;
    }


    /**
     * 单线程生产者 异步发送消息 没有回调
     */
    @Test
    public void seedAsyncTest(){
        ProducerRecord<String,String> record = null;
        StockInfo stockInfo = null;
        try {
            int num =0;
            for (int i=0;i<MSG_SIZE ;i++){
                stockInfo = createQuotationInfo();
                record = new ProducerRecord<String, String>(TOPIC,null,stockInfo.getTradeTime(),stockInfo.getStockCode(),stockInfo.toString());
                //异步发送消息
                producer.send(record);
                if(num++ % 10 == 0){
                    Thread.sleep(2000L);
                }
            }
        }catch (InterruptedException e){
            log.info(e.getMessage());
        }finally {
            producer.close();
        }
    }


    /**
     * 单线程 异步回调
     */
    @Test
    public void seedAsyncCallbackTest(){
        ProducerRecord<String,String> record = null;
        StockInfo stockInfo = null;
        try {
            int num =0;
            for (int i=0;i<MSG_SIZE ;i++){
                stockInfo = createQuotationInfo();
                record = new ProducerRecord<String, String>(TOPIC,null,stockInfo.getTradeTime(),stockInfo.getStockCode(),stockInfo.toString());
                //异步发送消息,并执行回调函数
                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(e != null){
                            //发送异常记录异常信息
                            log.info("发送异常------->>>>>>>"+e.getMessage());
                        }
                        if(recordMetadata != null){
                            log.info("发送成功------->>>>>>>"+"偏移量:"+recordMetadata.offset()+ "分区："+recordMetadata.partition());
                        }
                    }
                });
                if(num++ % 10 == 0){
                    Thread.sleep(2000L);
                }
            }
        }catch (InterruptedException e){
            log.info(e.getMessage());
        }finally {
            producer.close();
        }
    }


    /**
     * 多线程发送消息
     */
    @Test
    public void threadQuotationProducerTest(){
        ProducerRecord<String,String> record = null;
        StockInfo stockInfo = null;
        //五个线程
        final ExecutorService executor = Executors.newFixedThreadPool(5);

        final long l = System.currentTimeMillis();
        try {
            for (int i=0;i<MSG_SIZE;i++){
               stockInfo = createQuotationInfo();
               record = new ProducerRecord<>(TOPIC, null, stockInfo.getTradeTime(), stockInfo.getStockCode(), stockInfo.toString());
               executor.submit(new KafkaProducerThread(producer,record));
            }
        }catch (Exception e){
            log.info("异常"+e.toString());
        }finally {
            producer.close();
            executor.shutdown();

        }
    }



}
