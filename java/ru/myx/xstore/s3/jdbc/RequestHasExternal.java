/**
 *
 */
package ru.myx.xstore.s3.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import ru.myx.jdbc.queueing.RequestAttachment;
import ru.myx.jdbc.queueing.RunnerDatabaseRequestor;

/** @author myx */
public final class RequestHasExternal extends RequestAttachment<Boolean, RunnerDatabaseRequestor> {

	private final String key;

	/** @param guid
	 */
	public RequestHasExternal(final String guid) {

		this.key = guid;
	}

	@Override
	public final Boolean apply(final RunnerDatabaseRequestor ctx) {

		try (final PreparedStatement ps = ctx.ctxGetConnection()
				.prepareStatement("SELECT recId FROM s3Extra WHERE recId=?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
			ps.setString(1, this.key);
			final Boolean result;
			try (final ResultSet rs = ps.executeQuery()) {
				result = rs.next()
					? Boolean.TRUE
					: Boolean.FALSE;
			}
			this.setResult(result);
			return result;
		} catch (final SQLException e) {
			throw new RuntimeException(this.getClass().getSimpleName(), e);
		}
	}

	@Override
	public final String getKey() {

		return "he-" + this.key;
	}

}
