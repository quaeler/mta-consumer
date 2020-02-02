package st.theori.mta.kafka;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

import st.theori.mta.MTAMessage;

/**
 * Message format:
 * 	ABCDEFGHJK
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
public class MTAMessageDeserializer implements Deserializer<MTAMessage> {
	@Override
	public void configure (final Map<String, ?> map, final boolean b) { }

	@Override
	public MTAMessage deserialize (final String topic, final byte[] bytes) {
		int index = 0;

		DeserializedAtomWrapper atom = getNextAtom(index, bytes);
		final String trainId = atom.getContent();
		index = atom.getNewIndex();

		atom = getNextAtom(index, bytes);
		final String routeId = atom.getContent();
		index = atom.getNewIndex();

		atom = getNextAtom(index, bytes);
		final String status = atom.getContent();
		index = atom.getNewIndex();

		atom = getNextAtom(index, bytes);
		final String stop = atom.getContent();
		index = atom.getNewIndex();

		atom = getNextAtom(index, bytes);
		final String timestampString = atom.getContent();

		return new MTAMessage(trainId, routeId, status, stop, Long.parseLong(timestampString));
	}

	@Override
	public void close () { }

	private DeserializedAtomWrapper getNextAtom(int index, final byte[] bytes) {
		final int length = (new Byte(bytes[index++])).intValue();
		final byte[] content = new byte[length];
		System.arraycopy(bytes, index, content, 0, length);
		final String string = new String(content);

		return new DeserializedAtomWrapper(string, (index + length));
	}


	private static class DeserializedAtomWrapper {
		private final String content;
		private final int newIndex;

		private DeserializedAtomWrapper(final String string, final int index) {
			content = string;
			newIndex = index;
		}

		private String getContent() {
			return content;
		}

		private int getNewIndex() {
			return newIndex;
		}
	}
}
