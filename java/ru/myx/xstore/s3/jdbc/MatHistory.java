/*
 * Created on 20.08.2004
 *
 * Window - Preferences - Java - Code Style - Code Templates
 */
package ru.myx.xstore.s3.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import ru.myx.ae1.storage.BaseHistory;
import ru.myx.ae3.Engine;

/** @author myx
 *
 *         Window - Preferences - Java - Code Style - Code Templates */
final class MatHistory {

	static final void clear(final Connection conn, final String objId) throws Exception {

		try (final PreparedStatement ps = conn.prepareStatement("DELETE FROM s3ObjectHistory WHERE objId=?")) {
			ps.setString(1, objId);
			ps.execute();
		}
	}

	static final BaseHistory[] materialize(final Connection conn, final String objId) throws Exception {

		try (final PreparedStatement ps = conn
				.prepareStatement("SELECT hsId,hsDate,objTitle FROM s3ObjectHistory WHERE objId=? ORDER BY hsDate DESC", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
			ps.setString(1, objId);
			try (final ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					final List<BaseHistory> result = new ArrayList<>();
					do {
						result.add(new HistoryJdbc(rs.getString(1), rs.getTimestamp(2).getTime(), rs.getString(3)));
					} while (rs.next());
					return result.toArray(new BaseHistory[result.size()]);
				}
				return null;
			}
		}
	}

	static void record(final Connection conn, final String objId) throws Exception {

		final String historyId = Engine.createGuid();
		try (final PreparedStatement ps = conn.prepareStatement(
				"INSERT INTO s3ObjectHistory(hsId,hsDate,objId,vrId,objTitle,objCreated,objDate,objOwner,objType,objState,extLink) SELECT ?,?,objId,vrId,objTitle,objCreated,objDate,objOwner,objType,objState,extLink FROM s3Objects WHERE objId=?")) {
			ps.setString(1, historyId);
			ps.setTimestamp(2, new Timestamp(Engine.fastTime()));
			ps.setString(3, objId);
			ps.execute();
		}
		try (final PreparedStatement ps = conn.prepareStatement("INSERT INTO s3ExtraLink(objId,fldId,recId) SELECT ?,fldId,recId FROM s3ExtraLink WHERE objId=?")) {
			ps.setString(1, historyId);
			ps.setString(2, objId);
			ps.execute();
		}
	}
}
