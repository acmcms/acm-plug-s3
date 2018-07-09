/*
 * Created on 22.08.2004
 *
 * Window - Preferences - Java - Code Style - Code Templates
 */
package ru.myx.xstore.s3.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Set;

import ru.myx.ae3.help.Create;

/** @author myx
 *
 *         Window - Preferences - Java - Code Style - Code Templates */
final class MatAlias {

	static final void delete(final Connection conn, final String alId, final String alLnkId) throws Exception {

		if (alId == null) {
			try (final PreparedStatement ps = conn.prepareStatement("DELETE FROM s3Aliases WHERE alLnkId=?")) {
				ps.setString(1, alLnkId);
				ps.execute();
			}
		} else {
			try (final PreparedStatement ps = conn.prepareStatement("DELETE FROM s3Aliases WHERE alId=? AND alLnkId=?")) {
				ps.setString(1, alId);
				ps.setString(2, alLnkId);
				ps.execute();
			}
		}
	}

	static final void deleteOrMove(final Connection conn, final int lnkLuid) throws Exception {

		Set<String> aliases = null;
		String source = null;
		try (final PreparedStatement ps = conn.prepareStatement(
				"SELECT a.alId,t.lnkId FROM s3Aliases a,s3Tree t WHERE t.lnkLuid=? AND a.alLnkId=t.lnkId",
				ResultSet.TYPE_FORWARD_ONLY,
				ResultSet.CONCUR_READ_ONLY)) {
			ps.setInt(1, lnkLuid);
			try (final ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					if (aliases == null) {
						aliases = Create.tempSet();
					}
					aliases.add(rs.getString(1));
					source = rs.getString(2);
				}
				if (aliases == null) {
					return;
				}
			}
		}
		final String target;
		try (final PreparedStatement ps = conn.prepareStatement(
				"SELECT t2.lnkId FROM s3Tree t,s3Objects o,s3Tree t2 WHERE o.objId=t.objId AND o.objId=t2.objId AND t.lnkLuid=? AND t2.lnkLuid!=t.lnkLuid ORDER BY o.objState DESC",
				ResultSet.TYPE_FORWARD_ONLY,
				ResultSet.CONCUR_READ_ONLY)) {
			ps.setMaxRows(1);
			ps.setInt(1, lnkLuid);
			try (final ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					target = rs.getString(1);
				} else {
					target = null;
				}
			}
		}
		if (target == null) {
			try (final PreparedStatement ps = conn.prepareStatement("DELETE FROM s3Aliases WHERE alLnkId=?")) {
				ps.setString(1, source);
				ps.execute();
			}
		} else {
			try (final PreparedStatement ps = conn.prepareStatement("UPDATE s3Aliases SET alLnkId=? WHERE alLnkId=?")) {
				ps.setString(1, target);
				ps.setString(2, source);
				ps.execute();
			}
		}
	}

	static final void stayOrMove(final Connection conn, final int lnkLuid) throws Exception {

		Set<String> aliases = null;
		String source = null;
		try (final PreparedStatement ps = conn.prepareStatement(
				"SELECT a.alId,t.lnkId FROM s3Aliases a,s3Tree t WHERE t.lnkLuid=? AND a.alLnkId=t.lnkId",
				ResultSet.TYPE_FORWARD_ONLY,
				ResultSet.CONCUR_READ_ONLY)) {
			ps.setInt(1, lnkLuid);
			try (final ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					if (aliases == null) {
						aliases = Create.tempSet();
					}
					aliases.add(rs.getString(1));
					source = rs.getString(2);
				}
				if (aliases == null) {
					return;
				}
			}
		}
		final String target;
		try (final PreparedStatement ps = conn.prepareStatement(
				"SELECT t2.lnkId FROM s3Tree t,s3Objects o,s3Tree t2 WHERE o.objId=t.objId AND o.objId=t2.objId AND t.lnkLuid=? AND t2.lnkLuid!=t.lnkLuid ORDER BY o.objState DESC",
				ResultSet.TYPE_FORWARD_ONLY,
				ResultSet.CONCUR_READ_ONLY)) {
			ps.setMaxRows(1);
			ps.setInt(1, lnkLuid);
			try (final ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					target = rs.getString(1);
				} else {
					return;
				}
			}
		}
		{
			try (final PreparedStatement ps = conn.prepareStatement("UPDATE s3Aliases SET alLnkId=? WHERE alLnkId=?")) {
				ps.setString(1, target);
				ps.setString(2, source);
				ps.execute();
			}
		}
	}

	static final void update(final Connection conn, final String lnkId, final Set<String> added, final Set<String> removed) throws Exception {

		if (removed != null && !removed.isEmpty()) {
			try (final PreparedStatement ps = conn.prepareStatement("DELETE FROM s3Aliases WHERE alId=?")) {
				for (final String alias : removed) {
					ps.setString(1, alias);
					ps.addBatch();
				}
				ps.executeBatch();
			}
		}
		if (added != null && !added.isEmpty()) {
			try (final PreparedStatement ps = conn.prepareStatement("DELETE FROM s3Aliases WHERE alId=?")) {
				for (final String alias : added) {
					ps.setString(1, alias);
					ps.addBatch();
				}
				ps.executeBatch();
			}
			try (final PreparedStatement ps = conn.prepareStatement("INSERT INTO s3Aliases(alId,alLnkId) VALUES (?,?)")) {
				for (final String alias : added) {
					ps.setString(1, alias);
					ps.setString(2, lnkId);
					ps.addBatch();
				}
				ps.executeBatch();
			}
		}
	}
}
