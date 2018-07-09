/**
 *
 */
package ru.myx.xstore.s3.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import ru.myx.jdbc.queueing.RequestAttachment;
import ru.myx.jdbc.queueing.RunnerDatabaseRequestor;

/** @author myx */
public final class RequestAliases extends RequestAttachment<String[], RunnerDatabaseRequestor> {

	private final String key;

	/** @param guid
	 */
	public RequestAliases(final String guid) {

		this.key = guid;
	}

	@Override
	public final String[] apply(final RunnerDatabaseRequestor ctx) {

		try (final PreparedStatement ps = ctx.ctxGetConnection()
				.prepareStatement("SELECT alId FROM s3Aliases WHERE alLnkId=? ORDER BY alId ASC", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
			ps.setString(1, this.key);
			try (final ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					final List<String> result = new ArrayList<>();
					do {
						result.add(rs.getString(1));
					} while (rs.next());
					final String[] resultArray = result.toArray(new String[result.size()]);
					this.setResult(resultArray);
					return resultArray;
				}
				this.setResult(null);
				return null;
			}
		} catch (final SQLException e) {
			throw new RuntimeException(this.getClass().getSimpleName(), e);
		}
	}

	@Override
	public final String getKey() {

		return "al-" + this.key;
	}

}
