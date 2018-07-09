/*
 * Created on 20.08.2004
 *
 * Window - Preferences - Java - Code Style - Code Templates
 */
package ru.myx.xstore.s3.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import ru.myx.ae1.storage.BaseRecycled;
import ru.myx.ae3.Engine;
import ru.myx.ae3.report.Report;

/** @author myx
 *
 *         Window - Preferences - Java - Code Style - Code Templates */
final class MatRecycled {

	static final void clearRecycled(final Connection conn) throws Exception {

		try (final PreparedStatement ps = conn.prepareStatement("DELETE FROM s3Recycled")) {
			ps.execute();
		}
		try (final PreparedStatement ps = conn.prepareStatement("DELETE FROM s3RecycledTree")) {
			ps.execute();
		}
	}

	static final void clearRecycled(final Connection conn, final String guid) throws Exception {

		try (final PreparedStatement ps = conn.prepareStatement("DELETE FROM s3Recycled WHERE delRootId=?")) {
			ps.setString(1, guid);
			ps.execute();
		}
		try (final PreparedStatement ps = conn.prepareStatement("DELETE FROM s3RecycledTree WHERE delId=?")) {
			ps.setString(1, guid);
			ps.execute();
		}
	}

	static final BaseRecycled materialize(final ServerJdbc server, final Connection conn, final String giud) throws Exception {

		try (final PreparedStatement ps = conn.prepareStatement(
				"SELECT r.delRootId,r.delDate,o.objTitle,r.delCntId,r.delOwner FROM s3Recycled r,s3Objects o WHERE r.delRootId=? AND r.delObjId=o.objId ORDER BY r.delDate DESC",
				ResultSet.TYPE_FORWARD_ONLY,
				ResultSet.CONCUR_READ_ONLY)) {
			ps.setString(1, giud);
			try (final ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					return MatRecycled.materializeResultset(server, rs);
				}
				return null;
			}
		}
	}

	static final BaseRecycled[] materializeAll(final ServerJdbc server, final Connection conn) throws Exception {

		try (final PreparedStatement ps = conn.prepareStatement(
				"SELECT r.delRootId,r.delDate,o.objTitle,r.delCntId,r.delOwner FROM s3Recycled r,s3Objects o WHERE r.delObjId=o.objId ORDER BY r.delDate DESC",
				ResultSet.TYPE_FORWARD_ONLY,
				ResultSet.CONCUR_READ_ONLY)) {
			try (final ResultSet rs = ps.executeQuery()) {
				final List<BaseRecycled> result = new ArrayList<>();
				while (rs.next()) {
					result.add(MatRecycled.materializeResultset(server, rs));
				}
				if (result.isEmpty()) {
					return null;
				}
				return result.toArray(new BaseRecycled[result.size()]);
			}
		}
	}

	private static final BaseRecycled materializeResultset(final ServerJdbc server, final ResultSet rs) throws Exception {

		final String guid = rs.getString(1);
		final long date = rs.getTimestamp(2).getTime();
		final String title = rs.getString(3);
		final String folder = rs.getString(4);
		final String owner = rs.getString(5);
		return new RecycledJdbc(server, guid, date, title, folder, owner);
	}

	static final void recycleLink(final Connection conn, final String delId, final int lnkLuid) throws Exception {

		try (final PreparedStatement ps = conn.prepareStatement(
				"INSERT INTO s3RecycledTree(lnkId,delId,cntLnkId,lnkName,lnkFolder,objId) SELECT lnkId,?,cntLnkId,lnkName,lnkFolder,objId FROM s3Tree WHERE lnkLuid=?")) {
			ps.setString(1, delId);
			ps.setInt(2, lnkLuid);
			try {
				ps.execute();
			} catch (final SQLException e) {
				Report.warning("MAT_RECYCLER", "Problems recycling: delId=" + delId + ", lnkLuid=" + lnkLuid + ", msg=" + e.getMessage());
			}
		}
		MatLink.unlink(conn, lnkLuid);
	}

	static final void restoreRecycled(final ServerJdbc server, final Connection conn, final String guid) throws Exception {

		try (final PreparedStatement ps = conn
				.prepareStatement("INSERT INTO s3Tree(lnkId,cntLnkId,lnkName,lnkFolder,objId) SELECT lnkId,cntLnkId,lnkName,lnkFolder,objId FROM s3RecycledTree WHERE delId=?")) {
			ps.setString(1, guid);
			ps.execute();
		}
		try (final PreparedStatement ps = conn.prepareStatement(
				"INSERT INTO s3ChangeQueue(evtId,evtDate,evtSequence,evtOwner,evtCmdType,evtCmdGuid,evtCmdLuid) SELECT lnkId,?,0,?,?,lnkId,-1 FROM s3RecycledTree WHERE delId=?")) {
			ps.setTimestamp(1, new Timestamp(Engine.fastTime()));
			ps.setString(2, server.getIdentity());
			ps.setString(3, "create");
			ps.setString(4, guid);
			ps.execute();
		}
		MatRecycled.clearRecycled(conn, guid);
	}

	static final void restoreRecycled(final ServerJdbc server, final Connection conn, final String guid, final String targetId) throws Exception {

		try (final PreparedStatement ps = conn.prepareStatement(
				"INSERT INTO s3Tree(lnkId,cntLnkId,lnkName,lnkFolder,objId) SELECT lnkId,?,lnkName,lnkFolder,objId FROM s3RecycledTree WHERE delId=? AND lnkId=?")) {
			ps.setString(1, targetId);
			ps.setString(2, guid);
			ps.setString(3, guid);
			ps.execute();
		}
		try (final PreparedStatement ps = conn.prepareStatement("DELETE FROM s3RecycledTree WHERE delId=? AND lnkId=?")) {
			ps.setString(1, guid);
			ps.setString(2, guid);
			ps.execute();
		}
		MatChange.serialize(server, conn, 0, "create", guid, -1);
		try (final PreparedStatement ps = conn
				.prepareStatement("INSERT INTO s3Tree(lnkId,cntLnkId,lnkName,lnkFolder,objId) SELECT lnkId,cntLnkId,lnkName,lnkFolder,objId FROM s3RecycledTree WHERE delId=?")) {
			ps.setString(1, guid);
			ps.execute();
		}
		try (final PreparedStatement ps = conn.prepareStatement(
				"INSERT INTO s3ChangeQueue(evtId,evtDate,evtSequence,evtOwner,evtCmdType,evtCmdGuid,evtCmdLuid) SELECT lnkId,?,0,?,?,lnkId,-1 FROM s3RecycledTree WHERE delId=?")) {
			ps.setTimestamp(1, new Timestamp(Engine.fastTime()));
			ps.setString(2, server.getIdentity());
			ps.setString(3, "create");
			ps.setString(4, guid);
			ps.execute();
		}
		MatRecycled.clearRecycled(conn, guid);
	}
}
