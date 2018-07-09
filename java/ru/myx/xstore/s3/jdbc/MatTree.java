/*
 * Created on 12.07.2004
 */
package ru.myx.xstore.s3.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** @author myx */
final class MatTree {

	static final List<Object> children(final Connection conn, final int luid) throws Exception {

		try (final PreparedStatement ps = conn.prepareStatement(
				"SELECT t1.lnkLuid,t1.objId FROM s3Tree t1,s3Tree t2 WHERE t1.cntLnkId=t2.lnkId AND t2.lnkLuid=?",
				ResultSet.TYPE_FORWARD_ONLY,
				ResultSet.CONCUR_READ_ONLY)) {
			ps.setInt(1, luid);
			try (final ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					final List<Object> result = new ArrayList<>();
					do {
						result.add(new Integer(rs.getInt(1)));
						result.add(rs.getString(2));
					} while (rs.next());
					return result;
				}
				return Collections.emptyList();
			}
		}
	}

	static final List<Object> children(final Connection conn, final String lnkId) throws Exception {

		try (final PreparedStatement ps = conn.prepareStatement("SELECT lnkLuid,objId FROM s3Tree WHERE cntLnkId=?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
			ps.setString(1, lnkId);
			try (final ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					final List<Object> result = new ArrayList<>();
					do {
						result.add(new Integer(rs.getInt(1)));
						result.add(rs.getString(2));
					} while (rs.next());
					return result;
				}
				return Collections.emptyList();
			}
		}
	}
}
