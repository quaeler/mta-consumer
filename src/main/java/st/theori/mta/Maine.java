package st.theori.mta;

import java.io.BufferedInputStream;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.google.protobuf.ExtensionRegistry;
import com.google.transit.realtime.GtfsRealtime;
import com.google.transit.realtime.NycMtaExtensions;

import st.theori.mta.kafka.MTAKafkaProducer;

/**
 * This is the application which pulls events from the MTA feed and writes serialized MTA vehicle status messages to
 * 	our Kafka stream. This application never exits without some sort of process kill.
 *
 * Note to users, you must enter your appropriate MTA API keys in the class constants below.
 */
public class Maine {
	// at some point there was a hiccup in the MTA system where this feed stopped producing GTFS
	//		so this is currently unused - commented out at line 51
	private static final String MTA_URL = "http://datamine.mta.info/mta_esi.php?key=XXXX&feed_id=1";
	// I found that there is a new feed here (register at https://api.mta.info/)
	private static final String NEWER_MTA_URL = "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs";
	private static final String NEWER_HEADER_KEY = "x-api-key";
	private static final String NEWER_API_KEY = "XXXX";
	// dump each message to system.out?
	private static final boolean VERBOSE = true;
	private static final long POLLING_SLEEP = 5000;


	public static void main(final String[] args) throws Exception {
		Logger.getLogger("org").setLevel(Level.OFF);

		final MTAKafkaProducer producer = new MTAKafkaProducer();
		Runtime.getRuntime().addShutdownHook(new Thread(() -> producer.shutdownProducer()));

		final ExtensionRegistry registry = ExtensionRegistry.newInstance();
		registry.add(NycMtaExtensions.nyctFeedHeader);
		registry.add(NycMtaExtensions.nyctStopTimeUpdate);
		registry.add(NycMtaExtensions.nyctTripDescriptor);

		while (true) {
			final CloseableHttpClient httpclient = HttpClients.createDefault();
//			final HttpGet httpGet = new HttpGet(MTA_URL);
			final HttpGet httpGet = new HttpGet(NEWER_MTA_URL);
			httpGet.addHeader(NEWER_HEADER_KEY, NEWER_API_KEY);
			final CloseableHttpResponse response = httpclient.execute(httpGet);
			final HttpEntity entity = response.getEntity();
			final BufferedInputStream bis = new BufferedInputStream(entity.getContent());
			final GtfsRealtime.FeedMessage message = GtfsRealtime.FeedMessage.parseFrom(bis, registry);

			response.close();

			int statusCount = 0;
			for (final GtfsRealtime.FeedEntity fe : message.getEntityList()) {
				if (fe.hasVehicle()) {
					final GtfsRealtime.VehiclePosition vp = fe.getVehicle();
					final GtfsRealtime.TripDescriptor td = vp.getTrip();
					final NycMtaExtensions.NyctTripDescriptor mtaTD = td.getExtension(NycMtaExtensions.nyctTripDescriptor);
					final MTAMessage mtaMessage = new MTAMessage(mtaTD.getTrainId(),
																 td.getRouteId(),
																 vp.getCurrentStatus().toString(),
																 vp.getStopId(),
																 vp.getTimestamp());

					if (VERBOSE) {
						System.out.println("\tSending status: " + mtaMessage);
					}

					producer.sendMessage(mtaMessage);

					statusCount++;
				}
			}

			System.out.println("Got MTA update with: " + message.getEntityCount() + " entities, "
							   			+ statusCount + " of them were vehicle positions with status.");

			try {
				Thread.sleep(POLLING_SLEEP);
			} catch (final Exception e) { }
		}
	}
}
