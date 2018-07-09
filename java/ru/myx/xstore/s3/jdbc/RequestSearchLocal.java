/**
 *
 */
package ru.myx.xstore.s3.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import ru.myx.ae1.storage.ModuleInterface;
import ru.myx.ae3.report.Report;
import ru.myx.jdbc.queueing.RequestAttachment;
import ru.myx.jdbc.queueing.RunnerDatabaseRequestor;
import ru.myx.xstore.s3.concept.LinkData;

/** @author myx */
public final class RequestSearchLocal extends RequestAttachment<LinkData[], RunnerDatabaseRequestor> {

	private static final int STATE_ARCHIVE = ModuleInterface.STATE_ARCHIVE;

	private static final int STATE_PUBLISH = ModuleInterface.STATE_PUBLISH;

	private static final int STATE_SYSTEM = ModuleInterface.STATE_SYSTEM;

	private final String key;

	private final String query;

	/** @param key
	 * @param lnkId
	 * @param condition
	 */
	public RequestSearchLocal(final String key, final String lnkId, final String condition) {

		this.key = key;
		final StringBuilder query = new StringBuilder(128);
		query.append("SELECT t.lnkId,t.lnkName,t.lnkFolder,o.objTitle,o.objCreated,o.objDate,o.objType,o.objState FROM s3Tree t,s3Objects o WHERE t.cntLnkId='");
		query.append(lnkId);
		query.append("' AND t.objId=o.objId");
		if (condition != null && condition.length() > 0) {
			query.append(" AND (");
			query.append(condition);
			query.append(')');
		}
		query.append(" ORDER BY o.objTitle ASC");
		this.query = query.toString();
	}

	@Override
	public final LinkData[] apply(final RunnerDatabaseRequestor ctx) {

		try (final PreparedStatement ps = ctx.ctxGetConnection().prepareStatement(this.query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
			try (final ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					final List<LinkData> entries = new ArrayList<>();
					do {
						final String lnkId = rs.getString(1);
						final String lnkName = rs.getString(2);
						final boolean lnkFolder = "Y".equals(rs.getString(3));
						final String objTitle = rs.getString(4);
						final long objCreated = rs.getTimestamp(5).getTime();
						final long objModified = rs.getTimestamp(6).getTime();
						final String objType = rs.getString(7);
						final int objState = rs.getInt(8);
						final boolean listable = objState == RequestSearchLocal.STATE_SYSTEM || objState == RequestSearchLocal.STATE_PUBLISH;
						final boolean searchable = objState == RequestSearchLocal.STATE_ARCHIVE || objState == RequestSearchLocal.STATE_PUBLISH;
						final LinkData linkData = new LinkData(
								lnkId,
								-1,
								null,
								lnkName,
								lnkFolder,
								null,
								null,
								objTitle,
								objCreated,
								objModified,
								null,
								objType,
								objState,
								listable,
								searchable,
								null);
						entries.add(linkData);
					} while (rs.next());
					final LinkData[] result = entries.toArray(new LinkData[entries.size()]);
					this.setResult(result);
					return result;
				}
				this.setResult(null);
				return null;
			}
		} catch (final SQLException e) {
			Report.info("S3-RQ-NET-SI", this.query);
			throw new RuntimeException(this.getClass().getSimpleName(), e);
		}
	}

	@Override
	public final String getKey() {

		return this.key;
	}
}
