package st.theori.mta.kafka;

import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.ConsumerStrategy;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import scala.Tuple2;
import st.theori.mta.MTAMessage;
import st.theori.mta.janus.MTAGraphWrapper;

/**
 * This is the Spark executable which pulls our MTA vehicle status messages out of the Kafka stream and does a map
 * 	reduce on the status of the vehicles. spark-submit this class in the maven-assembled jar built by this project.
 */
public class SparkKafkaConsumer extends AbstractKafkaAgent {
	private static final boolean WRITE_TO_JANUS_GRAPH = true;

	public static void main(final String[] args) throws InterruptedException {
		Logger.getLogger("org").setLevel(Level.OFF);

		final Collection<String> topics = Arrays.asList(TOPIC_NAME);

		final MTAGraphWrapper graphWrapper;
		final SparkConf sparkConf = new SparkConf();
		sparkConf.setMaster("local[2]");
		sparkConf.setAppName("MTAKafkaConsumer");
		if (WRITE_TO_JANUS_GRAPH) {
			graphWrapper = new MTAGraphWrapper();
		} else {
			graphWrapper = null;
		}

		final JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(1));
		final ConsumerStrategy<Long, MTAMessage> strategy = ConsumerStrategies.Subscribe(topics, KAFKA_PARAMS);
		final JavaInputDStream<ConsumerRecord<Long, MTAMessage>> messages
				= KafkaUtils.createDirectStream(streamingContext, LocationStrategies.PreferConsistent(), strategy);
		final JavaPairDStream<Long, MTAMessage> results = messages.mapToPair(record -> new Tuple2<>(record.key(), record.value()));
		if (WRITE_TO_JANUS_GRAPH) {
			final JavaDStream<MTAMessage> mtaMessages = results.map(tuple2 -> tuple2._2());
			mtaMessages.foreachRDD(javaRDD -> {
				final List<MTAMessage> collectedMessages = javaRDD.collect();
				for (final MTAMessage message : collectedMessages) {
					graphWrapper.populateGraphFromMessage(message);
				}
			});
		}

		final JavaDStream<String> statuses = results.map(tuple2 -> tuple2._2().getStatus());
		final JavaPairDStream<String, Integer> statusCounts
				= statuses.mapToPair(s -> new Tuple2<>(s, 1)).reduceByKey((i1, i2) -> i1 + i2);
		statusCounts.foreachRDD(javaPairRDD -> {
			final Map<String, Integer> statusCountMap = javaPairRDD.collectAsMap();
			if (statusCountMap.size() > 0) {
				System.out.println("Update at " + (new Date()).toString());
			}
			for (final Map.Entry<String, Integer> me : statusCountMap.entrySet()) {
				System.out.println("Trains with status: " + me.getKey() + " : " + me.getValue());
			}
		});

		streamingContext.start();
		streamingContext.awaitTermination();
	}
}
