/*
 * Created on 12.07.2004
 */
package ru.myx.xstore.s3.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.Map;

import ru.myx.ae3.Engine;
import ru.myx.xstore.s3.StorageLevel3;
import ru.myx.xstore.s3.concept.InvalidationCollector;
import ru.myx.xstore.s3.concept.InvalidationEventType;

/** @author myx
 *
 *         To change the template for this generated type comment go to
 *         Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments */
final class MatChange {

	private static final long EXPIRE_EVENT = 1000L * 60L * 60L * 24L * 7L;

	static final void serialize(final ServerJdbc server, final Connection conn, final int sequence, final String type, final String guid, final int luid) throws SQLException {

		try (final PreparedStatement ps = conn.prepareStatement(
				"INSERT INTO s3ChangeQueue(evtId,evtDate,evtSequence,evtOwner,evtCmdType,evtCmdGuid,evtCmdLuid) VALUES (?,?," + sequence + ",?,?,?," + luid + ")")) {
			ps.setString(1, Engine.createGuid());
			ps.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
			ps.setString(3, server.getIdentity());
			ps.setString(4, type);
			ps.setString(5, guid);
			ps.execute();
		}
	}

	static final void serializeInvalidation(final StorageLevel3 server, final Connection conn, final InvalidationEventType type, final String guid) throws Exception {

		type.invalidateOn(server.getServerInterface(), guid);
		try (final PreparedStatement ps = conn
				.prepareStatement("INSERT INTO s3ChangeInfo(evtId,evtTarget,evtType,evtGuid,evtDate,evtExpire) SELECT ?,peerId,?,?,?,? FROM s3ChangePeer WHERE peerId!=?")) {
			ps.setString(1, Engine.createGuid());
			ps.setString(2, type.toString());
			ps.setString(3, guid);
			final long date = Engine.fastTime();
			ps.setTimestamp(4, new Timestamp(date));
			ps.setTimestamp(5, new Timestamp(date + MatChange.EXPIRE_EVENT));
			ps.setString(6, server.getIdentity());
			ps.execute();
		}
	}

	static final void serializeInvalidations(final StorageLevel3 server, final Connection conn, final InvalidationCollector invalidations) throws Exception {

		final Map<InvalidationEventType, Collection<String>> map = invalidations.getInvalidations();
		if (map == null) {
			return;
		}
		final boolean batchSupported = conn.getMetaData().supportsBatchUpdates();
		final long date = Engine.fastTime();
		final Timestamp evtDate = new Timestamp(date);
		final Timestamp evtExpire = new Timestamp(date + MatChange.EXPIRE_EVENT);
		try (final PreparedStatement ps = conn
				.prepareStatement("INSERT INTO s3ChangeInfo(evtId,evtTarget,evtType,evtGuid,evtDate,evtExpire) SELECT ?,peerId,?,?,?,? FROM s3ChangePeer WHERE peerId!=?")) {
			for (final Map.Entry<InvalidationEventType, Collection<String>> kind : map.entrySet()) {
				for (final String guid : kind.getValue()) {
					kind.getKey().invalidateOn(server.getServerInterface(), guid);
					ps.setString(1, Engine.createGuid());
					ps.setString(2, kind.getKey().toString());
					ps.setString(3, guid);
					ps.setTimestamp(4, evtDate);
					ps.setTimestamp(5, evtExpire);
					ps.setString(6, server.getIdentity());
					if (batchSupported) {
						ps.addBatch();
					} else {
						ps.execute();
					}
					ps.clearParameters();
				}
			}
			if (batchSupported) {
				try {
					ps.executeBatch();
				} catch (final Throwable t) {
					// ignore
				}
			}
		}
	}
}
