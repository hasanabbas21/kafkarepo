package com.bcbsma.api.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithCallback {

    public static void main(String[] args) {

        Properties properties = new Properties();

        final Logger logger = LoggerFactory.getLogger(ProducerWithCallback.class);

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        System.out.println("Hello World");

        KafkaProducer<String,String > producer = new KafkaProducer<String, String>(properties);

        for (int i=0; i<=9; i++){
            ProducerRecord<String,String > record =
                    new ProducerRecord<String, String>("first-topic","hello world " + i );

            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null){
                        logger.info("Received new metadaata \n " +
                                "Topic :" + recordMetadata.topic() + "\n" +
                                "Partition :" + recordMetadata.partition() + "\n" +
                                "Offset :" + recordMetadata.offset() + "\n" +
                                "Timestamsp :" + recordMetadata.timestamp() + "\n");
                    }
                    else {
                        logger.error("Error while producing message ");
                    }
                }
            });
        }
        producer.flush();
        producer.close();
    }
}
