package com.kafka.demo.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class consumer {

    private static KafkaConsumer<String,String> consumer = null;




    static {

        final Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"test");
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG,"test");

        properties.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG,"1024");
        //设置手动提交偏移量 -- false
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,true);

        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<>(properties);

    }


    /**
     * 按时间查询消费实例
     */
    @Test
    public void byTimeConsumerTest(){
        //订阅主题特定分区
        final TopicPartition topicPartition = new TopicPartition("stock-quotation", 0);
        consumer.assign(Arrays.asList(topicPartition));
        try {
            //设置查询12小时之前
            final HashMap<TopicPartition, Long> timestampsToSearch = new HashMap<>();
            timestampsToSearch.put(topicPartition,(System.currentTimeMillis()-12*3600*1000));
            final Map<TopicPartition, OffsetAndTimestamp> offsetMap = consumer.offsetsForTimes(timestampsToSearch);
            for (Map.Entry<TopicPartition,OffsetAndTimestamp> entry:offsetMap.entrySet()){
                final OffsetAndTimestamp value = entry.getValue();
                if(null != value){
                    //重置消费起始偏移量
                    consumer.seek(topicPartition,entry.getValue().offset());
                }
            }

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



    /**
     * 手动提交偏移量
     */
    @Test
    public void noAutoConsumerTest(){
        consumer.subscribe(Arrays.asList("stock-quotation"));
        int minCommitSize = 10;
        int iCount =10;
        try {
            while (true){
                //等待拉取消息
                final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String,String> record:records){
                    //模拟处理业务
                    System.out.println("消息>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>:partation:"+record.partition()+",offset:"+record.offset()+",key:"+record.key()+",value:"+record.value());
                    iCount++;
                }

                //业务处理后提交偏移量
                if(iCount >=minCommitSize){
                    consumer.commitAsync(new OffsetCommitCallback() {
                        @Override
                        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                            if(null == exception){
                                System.out.println("提交成功");
                            }else {
                                System.out.println("发生了异常");
                            }
                        }
                    });
                    iCount=0;
                }

            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            consumer.close();
        }


    }



    /**
     * 自动提交偏移量
     * 需要注释配置文件的 手动提交参数 参数
     */
    @Test
    public void autoConsumerTest(){
        consumer.subscribe(Arrays.asList("stock-quotation"));
        try {
            while (true){
                final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord record:records){
                    System.out.println("消息>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>:partation:"+record.partition()+",offset:"+record.offset()+",key:"+record.key()+",value:"+record.value());
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            consumer.close();

        }

    }










    @Test
    public void subscribeTopic(){
        consumer.subscribe(Arrays.asList("stock-quotation"), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                //提交消费者已拉取的消息偏移量
                consumer.commitSync();
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                //重置消费者对各分区已消费的偏移量到已提交的偏移量
                long committedOffset = -1;
                //final Map<TopicPartition, OffsetAndMetadata> committed = consumer.committed((Set<TopicPartition>) collection);
                for (TopicPartition topicPartition:collection){
                    //获取该分区已消费的偏移量
                    final long offset = consumer.committed(topicPartition).offset();
                    consumer.seek(topicPartition,offset+1);
                }
            }
        });

    }

}
