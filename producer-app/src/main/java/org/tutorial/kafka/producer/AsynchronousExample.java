/**
 * 
 */
package org.tutorial.kafka.producer;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * @author deeepak
 *
 */
public class AsynchronousExample {
	
	private final static String TOPIC = "test";
	private final static String BOOTSTRAP_SERVERS = 
			"localhost:9092,localhost:9093,localhost:9094";

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		
		produceMessage(5);

	}
	
	private static Producer<Long, String> createProducer(){
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		props.put(ProducerConfig.CLIENT_ID_CONFIG, "SampleExample");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		return new KafkaProducer<>(props);
	}
	
	private static void produceMessage(int messageCount) throws InterruptedException, ExecutionException {
		Producer<Long,String> producer = createProducer();
		
		long time = System.currentTimeMillis();
		final CountDownLatch countDownLatch = new CountDownLatch(messageCount);
		
		
		try {
			for(long index = time; index <time+ messageCount; index ++) {
				ProducerRecord<Long, String> record = 
						new ProducerRecord<Long, String>(TOPIC, index, "Send Message Test "+ index);
				
				producer.send(record, new ProduceCallback(record, time, countDownLatch));
				
				
				
			}
			
			countDownLatch.await(25, TimeUnit.SECONDS);
		}
		finally {
			producer.flush();
			producer.close();
		}
	}
	
	private static class ProduceCallback implements Callback{
		private ProducerRecord<Long, String> record;
		private long time;
		private CountDownLatch countDownLatch;
		
		private ProduceCallback(ProducerRecord<Long, String> record, long time, CountDownLatch countDownLatch) {
			this.record = record;
			this.time = time;
			this.countDownLatch = countDownLatch;
		}

		@Override
		public void onCompletion(RecordMetadata metadata, Exception e) {
			long elapsedTime = System.currentTimeMillis() - time;
			
			System.out.printf("sent record (key %s value %s) " + 
					"meta (partition = %d, offset = %d) time = %d \n", 
					record.key(),record.value(),metadata.partition(),metadata.offset(),elapsedTime);
			
			countDownLatch.countDown();
			
		}
		
	}

}
