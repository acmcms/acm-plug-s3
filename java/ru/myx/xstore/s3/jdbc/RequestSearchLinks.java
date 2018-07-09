/**
 *
 */
package ru.myx.xstore.s3.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Set;

import ru.myx.ae3.help.Create;
import ru.myx.jdbc.queueing.RequestAttachment;
import ru.myx.jdbc.queueing.RunnerDatabaseRequestor;
import ru.myx.xstore.s3.concept.LinkData;

/** @author myx */
public final class RequestSearchLinks extends RequestAttachment<LinkData[], RunnerDatabaseRequestor> {

	private final String key;

	/** @param guid
	 */
	public RequestSearchLinks(final String guid) {

		this.key = guid;
	}

	@Override
	public final LinkData[] apply(final RunnerDatabaseRequestor ctx) {

		Set<LinkData> result = null;
		final Connection conn = ctx.ctxGetConnection();
		try (final PreparedStatement ps = conn.prepareStatement(
				"SELECT t.lnkId,t.lnkFolder,o.objCreated,o.objDate,o.objState FROM s3Tree t,s3Objects o WHERE o.objId=? AND t.objId=o.objId",
				ResultSet.TYPE_FORWARD_ONLY,
				ResultSet.CONCUR_READ_ONLY)) {
			ps.setString(1, this.key);
			try (final ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					if (result == null) {
						result = Create.tempSet();
					}
					final String lnkId = rs.getString(1);
					final boolean lnkFolder = "Y".equals(rs.getString(2));
					final long objCreated = rs.getTimestamp(3).getTime();
					final long objModified = rs.getTimestamp(4).getTime();
					final int objState = rs.getInt(5);
					final LinkData linkData = new LinkData(lnkId, -1, null, null, lnkFolder, null, null, '?', objCreated, objModified, null, null, objState, false, false, null);
					result.add(linkData);
				}
			}
		} catch (final SQLException e) {
			throw new RuntimeException(this.getClass().getSimpleName(), e);
		}
		if (result == null) {
			this.setResult(null);
			return null;
		}
		final LinkData[] resultArray = result.toArray(new LinkData[result.size()]);
		this.setResult(resultArray);
		return resultArray;
	}

	@Override
	public final String getKey() {

		return "ls-" + this.key;
	}

}
