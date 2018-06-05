package com.flink.poc;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;

import com.example.Customer;
import com.flink.poc.avro.serialization.SchemaRegistryKeyedSerializationSchema;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * Purpose of this file. Please, change me!!!
 *
 * @author veselin.p
 */
public class ProducerSK {

    public static void main(String[] args) throws Exception {
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", "localhost:9092");
        kafkaProps.setProperty("group.id", "flink-schema-test-1");
        kafkaProps.setProperty("auto.offset.reset", "earliest");

        Map<String, String> configs = new HashMap<>();
        configs.put("key.deserializer", StringDeserializer.class.getName());
        configs.put("value.deserializer", KafkaAvroDeserializer.class.getName());
        configs.put("schema.registry.url", "http://127.0.0.1:8081");
        configs.put("specific.avro.reader", "true");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

        Customer cust1 = Customer
                .newBuilder()
                .setFirstName("Ivan")
                .setLastName("Ivanov")
                .setWeight(105f)
                .setHeight(180f)
                .setAge(32)
                .setEmail("new@mail.com")
                .setPhoneNumber("123-123-134")
                .build();

        // Create stream based on array of objects
        final DataStreamSource<Customer> customerDataStreamSource = env.fromElements(cust1);

        final SchemaRegistryKeyedSerializationSchema<Customer> keyedSerializationSchema = new SchemaRegistryKeyedSerializationSchema<>(configs, "keyfieldname");

        final FlinkKafkaProducer011<Customer> producer = new FlinkKafkaProducer011<>("customer-avro-flink", keyedSerializationSchema, kafkaProps);

        customerDataStreamSource.addSink(producer);

        env.execute();
    }
}
