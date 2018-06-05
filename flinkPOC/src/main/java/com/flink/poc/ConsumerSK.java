package com.flink.poc;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;

import com.example.Customer;
import com.flink.poc.avro.serialization.SchemaRegistryKeyedDeserializationSchema;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * Purpose of this file. Please, change me!!!
 *
 * @author veselin.p
 */
public class ConsumerSK {

    public static void main(String[] args) throws Exception {
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", "localhost:9092");
        kafkaProps.setProperty("group.id", "flink-schema-test-8");
        kafkaProps.setProperty("auto.offset.reset", "earliest");


        // avro part (deserializer)
        Map<String, String> configs = new HashMap<>();
        configs.put("key.deserializer", StringDeserializer.class.getName());
        configs.put("value.deserializer", KafkaAvroDeserializer.class.getName());
        configs.put("schema.registry.url", "http://127.0.0.1:8081");
        configs.put("specific.avro.reader", "true");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);


        final SchemaRegistryKeyedDeserializationSchema<Customer> deserializationSchema =
                new SchemaRegistryKeyedDeserializationSchema<>(
                Customer.class, Customer.getClassSchema(), configs);

        final DataStreamSource<Customer> dataSource =
                env.addSource(
                new FlinkKafkaConsumer011<>("customer-avro-flink", deserializationSchema, kafkaProps));
                //new FlinkKafkaConsumer011<>("test-avro", deserializationSchema, kafkaProps)); Use this to test the failing avro deserialization


        dataSource.map((MapFunction<Customer, String>) pCustomer -> pCustomer.toString()).print();

        env.execute();
    }


}
