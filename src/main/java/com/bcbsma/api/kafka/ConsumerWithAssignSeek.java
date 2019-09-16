package com.bcbsma.api.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ConsumerWithAssignSeek {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ConsumerWithAssignSeek.class.getName());
        Properties properties = new Properties();
        String topic = "first-topic";

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //Assign
        Long offsetToReadFrom = 15L;
        TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);
        consumer.assign(Arrays.asList(partitionToReadFrom));

        //Seek
        consumer.seek(partitionToReadFrom, offsetToReadFrom);

        int numMsgToRead = 5;
        boolean keepOnReading = true;
        int msgsRead = 0;

        while(keepOnReading){
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records ){

                msgsRead +=1;
                logger.info("Received message with \n " +
                        "Message Value :" + record.value() + "\n" +
                        "Partition :" + record.partition() + "\n" +
                        "Offset :" + record.offset() + "\n" +
                        "Timestamsp :" + record.timestamp() + "\n");

                if (msgsRead >= numMsgToRead){
                    keepOnReading = false;
                    break;
                }
            }

            }
        logger.info("Exiting the application");
        }

    }
