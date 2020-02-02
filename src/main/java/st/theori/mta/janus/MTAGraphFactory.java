package st.theori.mta.janus;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.EdgeLabel;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.Multiplicity;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.ConsistencyModifier;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.graphdb.database.StandardJanusGraph;

import com.google.common.base.Preconditions;

public class MTAGraphFactory {
	public static final String TIME_PROPERTY_KEY = "time";

	public static final String ARRIVED_EDGE = "arrived";
	public static final String CONNECTS_EDGE = "connects";
	public static final String SERVES_EDGE = "serves";

	public static final String LINE_LABEL = "line";
	public static final String STATION_LABEL = "station";
	public static final String TRAIN_LABEL = "train";

	private static final String MIXED_INDEX_NAME = "search";


	public static void load(final JanusGraph graph) {
		if (graph instanceof StandardJanusGraph) {
			Preconditions.checkState(((StandardJanusGraph)graph).getIndexSerializer().containsIndex(MIXED_INDEX_NAME),
									 "The indexing backend named \"%s\" is not defined in the StandardJanusGraph "
											+ "instance.", MIXED_INDEX_NAME);
		}

		final JanusGraphManagement management = graph.openManagement();

		// TODO there *has* to be a more coherent way to mark up the meta of the management with a version stamp
		//			instead of doing the following.
		if (management.containsVertexLabel(TRAIN_LABEL)
					&& management.containsVertexLabel(LINE_LABEL)
					&& management.containsVertexLabel(STATION_LABEL)) {
			System.out.println("Schema already exists.");

			return;
		}

		final PropertyKey name = management.makePropertyKey("name").dataType(String.class).make();
		// TODO we really want unique per vertex type - but for this MTA case will be able to probably get away with
		//			uniqueness across all vertices
		final JanusGraphManagement.IndexBuilder nameIndexBuilder = management.buildIndex("name", Vertex.class).addKey(name);
		nameIndexBuilder.unique();
		final JanusGraphIndex nameIndex = nameIndexBuilder.buildCompositeIndex();
		management.setConsistency(nameIndex, ConsistencyModifier.LOCK);

		final PropertyKey time = management.makePropertyKey(TIME_PROPERTY_KEY).dataType(Long.class).make();

		management.makeEdgeLabel(SERVES_EDGE).multiplicity(Multiplicity.MULTI).make();
		management.makeEdgeLabel(CONNECTS_EDGE).multiplicity(Multiplicity.SIMPLE).make();
		final EdgeLabel arrived = management.makeEdgeLabel(ARRIVED_EDGE).signature(time).make();
		management.buildEdgeIndex(arrived, "arrivalsByTime", Direction.BOTH, Order.desc, time);

		management.makeVertexLabel(TRAIN_LABEL).make();
		management.makeVertexLabel(LINE_LABEL).make();
		management.makeVertexLabel(STATION_LABEL).make();

		management.commit();
	}

	private MTAGraphFactory () {}
}
