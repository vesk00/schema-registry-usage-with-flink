package com.flink.poc.avro.serialization;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;

import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.net.ConnectException;
import java.util.HashMap;
import java.util.InputMismatchException;
import java.util.Map;


/**
 * Schema Registry Keyed Decirializer
 *
 * @author veselin.p
 */
public class SchemaRegistryKeyedDeserializationSchema<T> implements KeyedDeserializationSchema<T> {

    private Map<String, ?> configs;
    //private transient KafkaAvroDeserializer keyDeserializer;
    private transient KafkaAvroDeserializer valueDeserializer;
    private transient KafkaAvroDeserializer valueDeserializerGeneric;
    private final Class<T> avroType;
    private transient Schema readerSchema;
    private final String schemaString;


    /***
     *
     * @param configs key-value-pairs for {@link KafkaAvroDeserializer}
     *              schema.registry.url ({@link String}):
     *                  Comma-separated list of URLs for schema registry instances that can be used to register or look up schemas
     *              max.schemas.per.subject ({@link Integer}, default: 1000):
     *                  Maximum number of schemas to create or cache locally
     *
     */
    public SchemaRegistryKeyedDeserializationSchema(final Class<T> pAvroType, Schema pReaderSchema, Map<String, ?> configs) {
        avroType = pAvroType;
        readerSchema = pReaderSchema;
        schemaString = readerSchema.toString();
        this.configs = configs;
    }

    @Override
    public T deserialize(byte[] pMessageKey, byte[] pMessage, String pTopic, int pPartition, long pOffset)
            throws IOException {
        ensureInitialized();

        try {
            final Object readObj = valueDeserializer.deserialize(null, pMessage, readerSchema);

//            LOGGER.debug("Avro type is {}, Key {}, topic {}, partition {} offset {} and object read is {}!",
//                    avroType.getSimpleName(), pMessageKey, pTopic, pPartition, pOffset, readObj.toString());

            if (avroType.isInstance(readObj)) {
                return avroType.cast(readObj);
            } else {
                throw new InputMismatchException(String.format("The reader Schema does not match the avroType class. schema: %s", this.schemaString));
            }

//            return new GenericKeyValueRecord(
//                    (GenericRecord) keyDeserializer.deserialize(null, messageKey),
//                    (GenericRecord) valueDeserializer.deserialize(null, message)
//            );
        }
        catch(Exception e) {
            if (e.getCause() instanceof AvroTypeException) {
                final Object readObj = valueDeserializerGeneric.deserialize(null, pMessage);
                readObj.toString();
                //LOGGER.error ... and log the readObj.toString()
            }

//            LOGGER.error(
//                    "Deserialization failed for class {} with message key: {}, message {}, topic {}, partition {}, "
//                            + "offset {}!", avroType.getSimpleName(), pMessageKey, pMessage, pTopic, pPartition,
//                    pOffset, e);
//            return null;


            if (e.getCause() instanceof ConnectException)
                throw new ConnectException(
                        String.format("Connection to schema registry '%s' could not be established.",
                                this.configs.get("schema.registry.url")
                        )
                );
            else throw e;
        }
    }

    @Override
    public boolean isEndOfStream(T genericKeyValueRecord) {
        return false;
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return TypeExtractor.getForClass(avroType);
    }

    private void ensureInitialized() {

        if (this.readerSchema == null) {
            final Schema.Parser parser = new Schema.Parser();
            this.readerSchema = parser.parse(this.schemaString);
        }

        //        if(this.keyDeserializer == null) {
        //            this.keyDeserializer = new KafkaAvroDeserializer();
        //            this.keyDeserializer.configure(this.configs, true);
        //        }

        if(this.valueDeserializer == null) {
            this.valueDeserializer = new KafkaAvroDeserializer();
            // Note: KafkaAvroDeserializer has inside CachedSchemaRegistryClient
            this.valueDeserializer.configure(this.configs, false);
        }

        Map<String, String> patch = new HashMap<>();
        this.configs.forEach((k,v) -> {
            if (k == "specific.avro.reader" ) {
                patch.put("specific.avro.reader", "false");
            } else {
                patch.put(k,v.toString());
            }
        });

        if(this.valueDeserializerGeneric == null) {
            this.valueDeserializerGeneric = new KafkaAvroDeserializer();
            this.valueDeserializerGeneric.configure(patch, false);
        }
    }
}