/**
 *
 */
package ru.myx.xstore.s3.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Set;

import ru.myx.ae3.help.Create;
import ru.myx.ae3.help.Text;
import ru.myx.jdbc.queueing.RequestAttachment;
import ru.myx.jdbc.queueing.RunnerDatabaseRequestor;

/** @author myx */
public final class RequestHasExternals extends RequestAttachment<Set<String>, RunnerDatabaseRequestor> {

	private final String query;

	/** @param keys
	 */
	public RequestHasExternals(final Set<String> keys) {

		this.query = "SELECT recId FROM s3Extra WHERE recId in ('" + Text.join(keys, "','") + "')";
	}

	@Override
	public final Set<String> apply(final RunnerDatabaseRequestor ctx) {

		try (final PreparedStatement ps = ctx.ctxGetConnection().prepareStatement(this.query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
			Set<String> result = null;
			try (final ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					if (result == null) {
						result = Create.tempSet();
					}
					result.add(rs.getString(1));
				}
			}
			this.setResult(result);
			return result;
		} catch (final SQLException e) {
			throw new RuntimeException(this.getClass().getSimpleName(), e);
		}
	}

	@Override
	public final String getKey() {

		return null;
	}
}
