package com.kafka.demo.avro;

import com.sun.xml.internal.ws.encoding.soap.DeserializationException;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.util.Map;

public class AvroDeseriazlizer<T  extends SpecificRecordBase> implements Deserializer<T> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public T deserialize(String topic, byte[] data) {

        if(null == data){
            return null;
        }
        try {
            //获取该主题对应的实体类
            final SpecificRecordBase dataType = TopicEnum.getEnum(topic).getDataType();
            //得到schema实例化DatumReader
            final SpecificDatumReader<T> objectSpecificDatumReader = new SpecificDatumReader<>(dataType.getSchema());
            final BinaryDecoder binaryDecoder = DecoderFactory.get().directBinaryDecoder(new ByteArrayInputStream(data), null);
            return objectSpecificDatumReader.read(null,binaryDecoder);
        }catch (Exception e){
            System.out.println("反序列化");
            throw new DeserializationException(e.getMessage());
        }



    }


    @Override
    public void close() {

    }
}
