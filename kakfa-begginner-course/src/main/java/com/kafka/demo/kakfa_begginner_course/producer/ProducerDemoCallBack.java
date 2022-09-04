package com.kafka.demo.kakfa_begginner_course.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoCallBack {

	public static void main(String[] args) {
		final Logger logger = LoggerFactory.getLogger(ProducerDemoCallBack.class);
		// TODO Auto-generated method stub
		Properties prop = new Properties();
		prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);

		ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("first_topic",
				"Hello world 2!!");

		// Send Date -asysncronus
		producer.send(producerRecord, new Callback() {
			// Call back will always executes incase for successful records sent
			// or exception.
			public void onCompletion(RecordMetadata recordMetaDate, Exception e) {

				if (e == null) {
					logger.info("Kakfa Topic :-" + recordMetaDate.topic());

				} else {
					logger.error(e.getMessage());
				}

			}
		});

		producer.flush();
		producer.close();

	}

}
