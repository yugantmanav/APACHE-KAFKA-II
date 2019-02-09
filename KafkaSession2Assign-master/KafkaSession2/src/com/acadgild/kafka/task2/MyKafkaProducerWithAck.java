package com.acadgild.kafka.task2;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Properties;
import java.util.Scanner;
import java.util.Set;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class MyKafkaProducerWithAck {

	public static void main(String[] args) throws Exception {

		// Check arguments length value
		if (args.length == 0) {
			System.out.println("Include the topic name in the command");
			return;
		}

		// create instance for properties to access producer configs
		Properties props = new Properties();

		// Assign localhost id
		props.put("bootstrap.servers", "localhost:9092");

		// Set acknowledgements for producer requests.
		props.put("acks", "all");

		// If the request fails, the producer can automatically retry,
		props.put("retries", 3);

		// Specify buffer size in config
		props.put("batch.size", 16384);

		// Reduce the no of requests less than 0
		props.put("linger.ms", 1);

		// The buffer.memory controls the total amount of memory available to the
		// producer for buffering.
		props.put("buffer.memory", 33554432);

		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Scanner scanner = new Scanner(new File(args[0].toString()));
		Scanner datascanner = null;
		int index = 0;
		String topicname = "";
		String key = "";
		String value = "";
		ArrayList<String> topics = new ArrayList<String>();
		while (scanner.hasNextLine()) {
			datascanner = new Scanner(scanner.nextLine());
			datascanner.useDelimiter(args[1].toString());

			while (datascanner.hasNext()) {
				String data = datascanner.next();
				if (index == 0)
					topicname = data;
				else if (index == 1)
					key = data;
				else if (index == 2)
					value = data;
				else
					System.out.println("Invalid data");
				index++;
			}
			try {
				Producer<String, String> producer = new KafkaProducer<String, String>(props);
				producer.send(new ProducerRecord<String, String>(topicname, key,
				 value)).get();
				System.out.println("Message sent successfully");
				topics.add(topicname);
				producer.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
			index = 0;
		}
		Set<String> uniquetopics = new HashSet<String>(topics);
		com.acadgild.kafka.task2.MyKafkaConsumer.consumeData(new ArrayList(uniquetopics));
		
		scanner.close();
	}

}
