package st.theori.mta.kafka;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;

abstract class AbstractKafkaAgent {
	protected static final Map<String, Object> KAFKA_PARAMS;
	protected static final String TOPIC_NAME = "messages";

	static {
		KAFKA_PARAMS = new HashMap<>();

		KAFKA_PARAMS.put("bootstrap.servers", "localhost:9092");
		KAFKA_PARAMS.put("key.deserializer", LongDeserializer.class);
		KAFKA_PARAMS.put("key.serializer", LongSerializer.class);
		KAFKA_PARAMS.put("value.deserializer", MTAMessageDeserializer.class);
		KAFKA_PARAMS.put("value.serializer", MTAMessageSerializer.class);
		KAFKA_PARAMS.put("group.id", "mta.stream.1");
		KAFKA_PARAMS.put("auto.offset.reset", "latest");
		KAFKA_PARAMS.put("enable.auto.commit", Boolean.FALSE);
	}
}
