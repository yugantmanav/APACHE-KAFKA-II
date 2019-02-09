package com.acadgild.kafka.task2;

import java.util.ArrayList;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class MyKafkaConsumer {
	
	public static void consumeData(ArrayList topics) {
		
		Properties props = new Properties();

		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", "test");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "10000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

		// Kafka Consumer subscribes list of topics here.
		consumer.subscribe(topics);

		System.out.println("Subscribed to topic(s) " + topics);
		try {
			while (true) {
				consumer.poll(0);
				consumer.seekToBeginning(consumer.assignment());
				ConsumerRecords<String, String> records = consumer.poll(0);
				for (ConsumerRecord<String, String> record : records)

					// print the key and value for the consumer records.
					System.out.printf(" key = %s, value = %s\n", record.key(), record.value());
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			consumer.close();
		}
	}
}
