package st.theori.mta.kafka;

import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import st.theori.mta.MTAMessage;

/**
 * This is our publisher for the Kafka info-bus.
 */
public class MTAKafkaProducer extends AbstractKafkaAgent {
	private KafkaProducer producer;

	public MTAKafkaProducer() {
		producer = new KafkaProducer<>(KAFKA_PARAMS);
	}

	public void sendMessage (final MTAMessage message) throws Exception {
		try {
			final AtomicInteger bla;
			final long now = System.currentTimeMillis();
			final ProducerRecord<Long, MTAMessage> record = new ProducerRecord<>(TOPIC_NAME, new Long(now), message);
			final Future<RecordMetadata> f = producer.send(record);
			final RecordMetadata metadata = f.get();
		} finally {
			producer.flush();
		}
	}

	public void shutdownProducer() {
		producer.close();
		System.out.println("KafkaProducer shutdown.");
	}
}
