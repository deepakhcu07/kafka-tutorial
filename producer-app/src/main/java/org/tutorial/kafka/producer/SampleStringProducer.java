package org.tutorial.kafka.producer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class SampleStringProducer {
	
	public static final String TOPIC_NAME  = "test";

	public static void main(String []args) {
		
		//Connect to the Kafka Cluster
		
		//Setting Kafka Properties
		Properties kafkaProps = new Properties();
		
		kafkaProps.put("bootstrap.servers","localhost:9092");
		kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		
		KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaProps);
		
		//ProducerRecord<String, String> record = new ProducerRecord<String, String>(TOPIC_NAME, "Message 1","This is Test Message 2");
		
		
		List<ProducerRecord<String, String>> recordList = sampleMessages();
		
		long l = System.currentTimeMillis();
		for(ProducerRecord<String, String> record: recordList) {
			
			try {
				producer.send(record);
			}catch(Exception e) {
				e.printStackTrace();
			}
			
		}
		
		System.out.println("Total time taken = " + (System.currentTimeMillis()-l));
		
		producer.close();
		
	}
	
	private static List<ProducerRecord<String, String>> sampleMessages(){
		List<ProducerRecord<String, String>> recordList = new ArrayList<>();
		for(int i=1;i<=10000;i++) {
			String key = "Key " + i;
			String message = "Message " + i;
			ProducerRecord<String, String> record = new 
					ProducerRecord<String, String>(TOPIC_NAME, key,message);
			recordList.add(record);
		}
		return recordList;
	}
}
