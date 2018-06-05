package com.flink.poc.avro.serialization;

import io.confluent.kafka.serializers.KafkaAvroSerializer;

import java.util.Map;

import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;

/**
 * Purpose of this file. Please, change me!!!
 *
 * @author veselin.p
 */
public class SchemaRegistryKeyedSerializationSchema<T> implements KeyedSerializationSchema<T> {

    private Map<String, ?> configs;
    private transient KafkaAvroSerializer valueSerializer;
    private final String keyFieldName;


    public SchemaRegistryKeyedSerializationSchema(Map<String, ?> configs, final String pKeyFieldName) {
        this.configs = configs;
        this.keyFieldName = pKeyFieldName;
    }

    @Override
    public byte[] serializeKey(final T pT) {
        // TODO use the keyFieldName as up to now
        return new byte[0];
    }



    @Override
    public byte[] serializeValue(final T pT) {
        if(this.valueSerializer == null) {
            this.valueSerializer = new KafkaAvroSerializer();
            this.valueSerializer.configure(this.configs, false);
        }
        return valueSerializer.serialize("customer-avro-flink", pT);
    }



    @Override
    public String getTargetTopic(final T pT) {
        return null;
    }
}

