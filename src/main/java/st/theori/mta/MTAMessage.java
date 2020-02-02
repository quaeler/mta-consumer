package st.theori.mta;

import java.io.Serializable;
import java.util.Date;

/**
 * A simple distilling of a single MTA status message concerning a vehicle (serializable for participating in Spark's
 * 	RDD collection tasks.)
 */
public class MTAMessage implements Serializable {
	private static final long serialVersionUID = -3377454485573207163L;


	private final String trainId;
	private final String routeId;
	private final String status;
	private final String stopId;

	// note that this is *not* in milliseconds - in seconds instead
	private final long timestampInSeconds;

	/**
	 * @param train the train uid
	 * @param route the line uid (which is also the human displayable text)
	 * @param currentStatus the current status of the train, which is the toString() of the protobuf'd enum
	 * @param stop the station uid
	 * @param timestamp the timestamp of the event which is <b>seconds</b> since epoch, per proto definition
	 */
	public MTAMessage(final String train, final String route, final String currentStatus, final String stop,
					  final long timestamp) {
		trainId = train;
		routeId = route;
		status = currentStatus;
		stopId = stop;
		timestampInSeconds = timestamp;
	}

	/**
	 * @return the train uid
	 */
	public String getTrainId () {
		return trainId;
	}

	/**
	 * @return the line uid (which is also the human displayable text)
	 */
	public String getRouteId () {
		return routeId;
	}

	/**
	 * @return the current status of the train, which is the toString() of the protobuf'd enum
	 * @see com.google.transit.realtime.GtfsRealtime.VehiclePosition.VehicleStopStatus
	 */
	public String getStatus () {
		return status;
	}

	/**
	 * @return the station uid
	 */
	public String getStopId () {
		return stopId;
	}

	/**
	 * @return <b>seconds</b> since epoch
	 */
	public long getTimestampInSeconds () {
		return timestampInSeconds;
	}

	/**
	 * @return a 'pretty' diagnostic text
	 */
	@Override
	public String toString() {
		return "Vehicle: " + trainId + " on route: " + routeId + " has status: " + status + " stop: " + stopId
						   + " at: " + (new Date(timestampInSeconds * 1000)).toString();
	}
}
