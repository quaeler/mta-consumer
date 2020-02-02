package st.theori.mta.kafka;

import java.io.ByteArrayOutputStream;
import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;

import st.theori.mta.MTAMessage;

/**
 * Message format:
 * 	ABCDEFGH
 *
 * 	A: 1 byte representing number of bytes to read for B
 * 	B: default platform encoding String bytes for the train id described by the message
 * 	C: 1 byte representing number of bytes to read for D
 * 	D: default platform encoding String bytes for the route described by the message
 * 	E: 1 byte representing number of bytes to read for F
 * 	F: default platform encoding String bytes for the status described by the message
 * 	G: 1 byte representing number of bytes to read for H
 * 	H: default platform encoding String bytes for the stop (impending or present) described by the message
 * 	J: 1 byte representing number of bytes to read for K
 * 	K: default platform encoding String bytes for the string version of the long timestamp described by the message
 */
public class MTAMessageSerializer implements Serializer<MTAMessage> {
	@Override
	public void configure (final Map<String, ?> map, final boolean b) { }

	@Override
	public byte[] serialize (final String s, final MTAMessage mtaMessage) {
		final ByteArrayOutputStream baos = new ByteArrayOutputStream();
		final byte[] trainBA = mtaMessage.getTrainId().getBytes();
		final byte trainLength = (new Integer(trainBA.length)).byteValue();
		final byte[] routeBA = mtaMessage.getRouteId().getBytes();
		final byte routeLength = (new Integer(routeBA.length)).byteValue();
		final byte[] statusBA = mtaMessage.getStatus().getBytes();
		final byte statusLength = (new Integer(statusBA.length)).byteValue();
		final byte[] stopBA = mtaMessage.getStopId().getBytes();
		final byte stopLength = (new Integer(stopBA.length)).byteValue();
		final String timestamp = Long.toString(mtaMessage.getTimestampInSeconds());
		final byte[] timestampBA = timestamp.getBytes();
		final byte timestampLength = (new Integer(timestampBA.length)).byteValue();

		baos.write(trainLength);
		baos.write(trainBA, 0, trainLength);
		baos.write(routeLength);
		baos.write(routeBA, 0, routeLength);
		baos.write(statusLength);
		baos.write(statusBA, 0, statusLength);
		baos.write(stopLength);
		baos.write(stopBA, 0, stopLength);
		baos.write(timestampLength);
		baos.write(timestampBA, 0, timestampLength);

		return baos.toByteArray();
	}

	@Override
	public void close () { }
}
