package com.couchbase.connect.kafka.example;

import org.apache.kafka.connect.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.couchbase.client.dcp.message.DcpMutationMessage;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.connect.kafka.dcp.EventType;
import com.couchbase.connect.kafka.handler.source.CouchbaseSourceRecord;
import com.couchbase.connect.kafka.handler.source.DefaultSchemaSourceHandler;
import com.couchbase.connect.kafka.handler.source.DocumentEvent;
import com.couchbase.connect.kafka.handler.source.SourceHandlerParams;

import static com.couchbase.connect.kafka.converter.ConverterUtils.bufToBytes;

public class RawBytesSourceHandler extends DefaultSchemaSourceHandler {

	private static final Logger LOGGER = LoggerFactory.getLogger(RawBytesSourceHandler.class);

	@Override
	protected boolean buildValue(SourceHandlerParams params, CouchbaseSourceRecord.Builder builder) {
		final DocumentEvent docEvent = params.documentEvent();
		final ByteBuf event = docEvent.rawDcpEvent();

		final EventType type = EventType.of(event);
		if (type == EventType.MUTATION) {
			byte[] bytes = bufToBytes(DcpMutationMessage.content(event));
			builder.value(Schema.BYTES_SCHEMA, bytes);
		} else if (type == EventType.DELETION) {
			builder.value(Schema.BYTES_SCHEMA, null);
		} else if (type == EventType.EXPIRATION) {
			builder.value(Schema.BYTES_SCHEMA, null);
		} else {
			LOGGER.warn("unexpected event type {}", event.getByte(1));
			return false;
		}
		return true;
	}
}
