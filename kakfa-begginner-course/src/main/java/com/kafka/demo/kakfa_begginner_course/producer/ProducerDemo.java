package com.kafka.demo.kakfa_begginner_course.producer;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerDemo {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		// Steps to create producer 
		//1. Set properties
		//2. Create the producer
		//3. Send Data ; Create Record
		Properties prop=new Properties();
		prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//		prop.setProperty(ProducerConfig.GROUP_ID_CONFIG, "my-first-application");

	
		KafkaProducer<String, String> producer=new KafkaProducer<String, String> (prop);
		
		ProducerRecord<String, String> producerRecord= new ProducerRecord<String, String>("first_topic","Hello world!!");
		
		// Send Date -asysncronus
		producer.send(producerRecord);
		
		producer.flush();
		producer.close();
		
	}

}
