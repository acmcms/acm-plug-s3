/**
 *
 */
package ru.myx.xstore.s3.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Enumeration;

import ru.myx.jdbc.queueing.RunnerDatabaseRequestor;

/** @author myx */
public final class RunnerTreeLoader extends RunnerDatabaseRequestor {

	private PreparedStatement ps;

	RunnerTreeLoader(final String domain, final Enumeration<Connection> connectionSource) {

		super(domain + ":S3-TREE", connectionSource);
	}

	/** @return tree load statement */
	public final PreparedStatement getStatement() {

		return this.ps;
	}

	@Override
	protected final void loopPrepare() throws Exception {

		this.ps = this.ctxGetConnection().prepareStatement(
				"SELECT t.lnkId,t.lnkName,t.lnkFolder,o.objTitle,o.objCreated,o.objDate,o.objType,o.objState FROM s3Tree t,s3Objects o WHERE t.cntLnkId=? AND t.objId=o.objId ORDER BY o.objTitle ASC",
				ResultSet.TYPE_FORWARD_ONLY,
				ResultSet.CONCUR_READ_ONLY);
	}

	@Override
	protected final void loopRelease() {

		if (this.ps != null) {
			try {
				this.ps.close();
			} catch (final Throwable t) {
				// ignore
			} finally {
				this.ps = null;
			}
		}
	}

}
