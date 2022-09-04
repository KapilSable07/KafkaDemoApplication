package com.kafka.demo.kakfa_begginner_course.cosumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaConsumerSignSeek {

	public static void main(String[] args) {

		// Using assign and seek for consumer
		// Assign and seek mostly used to replay Date or fetch a specific
		// message.

		final Logger logger = LoggerFactory.getLogger(KafkaConsumerDemo.class);

		Properties prop = new Properties();
		prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		// prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG,
		// "my-second-application");
		prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(prop);

		TopicPartition partitionToReadFrom = new TopicPartition("first_topic", 0);
		consumer.assign(Arrays.asList(partitionToReadFrom));

		long offSetToreadFrom = 5L;

		consumer.seek(partitionToReadFrom, offSetToreadFrom);

		int numberMessageToRead = 5;
		boolean keepOnReading = true;
    int numberOfMessageReasSoFar=0;
		// consumer.subscribe(Arrays.asList("first_topic"));

		while (keepOnReading) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

			for (ConsumerRecord<String, String> record : records) {

				logger.info("Key :-" + record.key() + "  Value :-" + record.value());

				if(numberOfMessageReasSoFar>=numberMessageToRead){
					keepOnReading=false;
					break;
				}
				
				
			}

		}
		
		consumer.close();
		

	}

}
