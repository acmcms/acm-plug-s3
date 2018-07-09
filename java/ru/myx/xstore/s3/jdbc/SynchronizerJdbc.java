/*
 * Created on 01.08.2004
 */
package ru.myx.xstore.s3.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Set;

import ru.myx.ae1.storage.BaseSync;
import ru.myx.ae1.storage.ModuleSynchronizer;
import ru.myx.ae3.help.Create;

/** @author myx
 *
 *         Window - Preferences - Java - Code Style - Code Templates */
final class SynchronizerJdbc implements ModuleSynchronizer {

	static final void commitChange(final Connection conn, final String guid, final Set<String> e1, final Set<String> i1, final Set<String> e2, final Set<String> i2)
			throws SQLException {

		for (final String key : e1) {
			if (!e2.contains(key)) {
				try (final PreparedStatement ps = conn.prepareStatement("DELETE FROM s3TreeSync WHERE lnkSrcId=? AND lnkTgtId=?")) {
					ps.setString(1, guid);
					ps.setString(2, key);
					ps.execute();
				}
			}
		}
		for (final String key : i1) {
			if (!i2.contains(key)) {
				try (final PreparedStatement ps = conn.prepareStatement("DELETE FROM s3TreeSync WHERE lnkSrcId=? AND lnkTgtId=?")) {
					ps.setString(1, key);
					ps.setString(2, guid);
					ps.execute();
				}
			}
		}
		for (final String key : e2) {
			if (!e1.contains(key)) {
				try (final PreparedStatement ps = conn.prepareStatement("INSERT  INTO s3TreeSync (lnkSrcId, lnkTgtId) VALUES (?,?)")) {
					ps.setString(1, guid);
					ps.setString(2, key);
					ps.execute();
				}
			}
		}
		for (final String key : i2) {
			if (!i1.contains(key)) {
				try (final PreparedStatement ps = conn.prepareStatement("INSERT  INTO s3TreeSync (lnkSrcId, lnkTgtId) VALUES (?,?)")) {
					ps.setString(1, key);
					ps.setString(2, guid);
					ps.execute();
				}
			}
		}
	}

	private final ServerJdbc server;

	SynchronizerJdbc(final ServerJdbc server) {

		this.server = server;
	}

	void commitChange(final String guid, final Set<String> e1, final Set<String> i1, final Set<String> e2, final Set<String> i2) {

		try (final Connection conn = this.server.getStorage().nextConnection()) {
			SynchronizerJdbc.commitChange(conn, guid, e1, i1, e2, i2);
		} catch (final SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public BaseSync createChange(final String guid) {

		try (final Connection conn = this.server.getStorage().nextConnection()) {
			try (final PreparedStatement ps = conn
					.prepareStatement("SELECT lnkSrcId,lnkTgtId FROM s3TreeSync WHERE lnkSrcId=? OR lnkTgtId=?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
				ps.setString(1, guid);
				ps.setString(2, guid);
				try (final ResultSet rs = ps.executeQuery()) {
					final Set<String> exportList = Create.tempSet();
					final Set<String> importList = Create.tempSet();
					while (rs.next()) {
						final String srcId = rs.getString(1);
						final String tgtId = rs.getString(2);
						if (guid.equals(srcId)) {
							exportList.add(tgtId);
						} else {
							importList.add(srcId);
						}
					}
					return new Synchronization(this, guid, exportList, importList);
				}
			}
		} catch (final SQLException e) {
			throw new RuntimeException(e);
		}
	}
}
