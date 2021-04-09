package com.kafka.demo.avro;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.commons.lang3.StringUtils;

public enum TopicEnum {

    STOCK_QUOTATION_AVRO("stock_quotation_avro", new AvroStockQuotation());
    public String topicName;
    public SpecificRecordBase dataType;


    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public SpecificRecordBase getDataType() {
        return dataType;
    }

    public void setDataType(SpecificRecordBase dataType) {
        this.dataType = dataType;
    }

    public static TopicEnum getEnum(String topicName){
        if(StringUtils.isBlank(topicName)){
            return null;
        }
        for (TopicEnum topic:values()){
            if(StringUtils.equals(topic.getTopicName(),topicName)){
                return topic;
            }
        }
        return null;
    }
}
