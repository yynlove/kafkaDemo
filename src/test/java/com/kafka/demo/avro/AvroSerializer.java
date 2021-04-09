package com.kafka.demo.avro;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import javax.sql.rowset.serial.SerialException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

public class AvroSerializer<T extends SpecificRecordBase> implements Serializer<T> {


    @Override
    public byte[] serialize(String topic, T data) {
        if(null == data){
            return null;
        }
        final SpecificDatumWriter<Object> objectSpecificDatumWriter = new SpecificDatumWriter<>(data.getSchema());
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        final BinaryEncoder binaryEncoder = EncoderFactory.get().directBinaryEncoder(byteArrayOutputStream, null);
        try {
            objectSpecificDatumWriter.write(data,binaryEncoder);
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
        return byteArrayOutputStream.toByteArray();
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }


    @Override
    public void close() {

    }
}
