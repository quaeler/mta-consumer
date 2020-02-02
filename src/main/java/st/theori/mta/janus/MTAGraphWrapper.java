package st.theori.mta.janus;

import java.util.NoSuchElementException;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;

import com.google.transit.realtime.GtfsRealtime;

import st.theori.mta.MTAMessage;

/**
 * A wrapper for our MTA Graph sitting in JanusGraph; the graph will be appropriately closed on app shutdown.
 */
public class MTAGraphWrapper {
	private static final String CONFIG_FILE = "/janus-conf/janusgraph-cassandra-es.properties";

	private final GraphTraversalSource graphTraversalSource;
	private final JanusGraph graph;

	public MTAGraphWrapper () {
		try {
			final PropertiesConfiguration pfc = new PropertiesConfiguration(getClass().getResource(CONFIG_FILE));
			graph = JanusGraphFactory.open(pfc);
		} catch (final ConfigurationException e) {
			throw new IllegalStateException("Encountered a problem loading the graph configuration file.", e);
		}
		graphTraversalSource = graph.traversal();
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			try {
				graphTraversalSource.close();
			} catch (final Exception e) {
				System.out.println("During shutdown, caught exception closing the graph traversal source: "
								   + e.getMessage());
			}
		}));

		MTAGraphFactory.load(graph);
		System.out.println("Schema created.");
	}

	public void populateGraphFromMessage (final MTAMessage message) {
		if (GtfsRealtime.VehiclePosition.VehicleStopStatus.STOPPED_AT.toString().equals(message.getStatus())) {
			final Vertex station = getVertexForNameAndType(message.getStopId(), MTAGraphFactory.STATION_LABEL, null);
			final Vertex train = getVertexForNameAndType(message.getTrainId(), MTAGraphFactory.TRAIN_LABEL, station);
			final Vertex line = getVertexForNameAndType(message.getRouteId(), MTAGraphFactory.LINE_LABEL, station);

			train.addEdge(MTAGraphFactory.ARRIVED_EDGE, station, MTAGraphFactory.TIME_PROPERTY_KEY,
						  new Long(message.getTimestampInSeconds()));

			graphTraversalSource.tx().commit();
		}
	}

	private Vertex getVertexForNameAndType(final String name, final String type, final Vertex stationVertex) {
		try {
			final Vertex v = graphTraversalSource.V().has("name", name).next();

			if (stationVertex != null) {
				try {
					if (!graphTraversalSource.V(v.id()).outE(MTAGraphFactory.SERVES_EDGE).inV().next().id().equals(stationVertex.id())) {
						v.addEdge(MTAGraphFactory.SERVES_EDGE, stationVertex);
						System.out.println("added edge to station for " + type + " with name " + name
										   	+ " as it didn't exist before.");
					}
				} catch (final NoSuchElementException e) {
					v.addEdge(MTAGraphFactory.SERVES_EDGE, stationVertex);
					System.out.println("added edge to station as it didn't exist before #2.");
				}
			}

			return v;
		} catch (final NoSuchElementException e) {
			final Vertex v = graphTraversalSource.addV(type).property("name", name).next();
			if (stationVertex != null) {
				v.addEdge(MTAGraphFactory.SERVES_EDGE, stationVertex);
			}
			System.out.println("added " + type + " for " + name);

			return v;
		}
	}
}
