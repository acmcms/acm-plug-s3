/**
 *
 */
package ru.myx.xstore.s3.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.TreeMap;

import ru.myx.ae1.storage.ModuleInterface;
import ru.myx.ae3.act.ActService;
import ru.myx.ae3.report.Report;
import ru.myx.util.FifoQueueMultithreadEnqueue;
import ru.myx.util.FifoQueueServiceMultithreadSwitching;
import ru.myx.xstore.s3.concept.LinkData;

/** @author myx */
final class RunnerLinkLoader implements ActService {

	private static final int STATE_SYSTEM = ModuleInterface.STATE_SYSTEM;

	private static final int STATE_ARCHIVE = ModuleInterface.STATE_ARCHIVE;

	private static final int STATE_PUBLISH = ModuleInterface.STATE_PUBLISH;

	private static final boolean formatSqlStringParameter(final StringBuilder builder, final String parameter, final boolean first) {

		if (parameter == null) {
			return first;
		}
		final int length = parameter.length();
		if (length < 1) {
			return first;
		}
		if (!first) {
			builder.append(',');
		}
		builder.append('\'');
		loop : for (int i = 0; i < length; ++i) {
			final char c = parameter.charAt(i);
			if (c == '\'') {
				builder.append(c).append(c);
				continue loop;
			}
			if (c == 0) {
				builder.append('\'');
				return false;
			}
			{
				builder.append(c);
				continue loop;
			}
		}
		builder.append('\'');
		return false;
	}

	private final ServerJdbc server;

	private final FifoQueueServiceMultithreadSwitching<LinkData> queue = new FifoQueueServiceMultithreadSwitching<>();

	private boolean destroyed = false;

	RunnerLinkLoader(final ServerJdbc server) {

		this.server = server;
	}

	@Override
	public final boolean main() throws Throwable {

		/** wait for the first task */
		this.queue.switchPlanesWaitReady(0L);

		final Map<String, LinkData> map = new TreeMap<>();
		try (final Connection conn = this.server.getStorage().nextConnection()) {
			try (final Statement st = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
				for (int loops = 256; loops > 0; loops--) {
					/** linger for 2500 millis */
					final FifoQueueMultithreadEnqueue<LinkData> queue = this.queue.switchPlanesWait(2500L);
					if (queue == null) {
						break;
					}
					final StringBuilder query = new StringBuilder(
							"SELECT t.lnkId,t.lnkLuid,t.cntLnkId,t.lnkName,t.lnkFolder,t.objId,o.vrId,o.objTitle,o.objCreated,o.objDate,o.objOwner,o.objType,o.objState,o.extLink FROM s3Tree t,s3Objects o WHERE t.objId=o.objId AND t.lnkId IN(");
					try {
						boolean first = true;
						for (LinkData link; (link = queue.pollFirst()) != null;) {
							final LinkData other = map.put(link.lnkId, link);
							if (other == null) {
								first = RunnerLinkLoader.formatSqlStringParameter(query, link.lnkId, first);
							} else {
								other.setDuplicateOf(link);
							}
						}

						if (!first) {
							query.append(')');
							try (final ResultSet rs = st.executeQuery(query.toString())) {
								while (rs.next()) {
									final String guid = rs.getString(1);
									final LinkData link = map.remove(guid);
									if (link != null) {
										final int lnkLuid = rs.getInt(2);
										final String lnkCntId = rs.getString(3);
										final String lnkName = rs.getString(4);
										final boolean lnkFolder = "Y".equals(rs.getString(5));
										final String objId = rs.getString(6);
										final String vrId = rs.getString(7);
										final String objTitle = rs.getString(8);
										final long objCreated = rs.getTimestamp(9).getTime();
										final long objDate = rs.getTimestamp(10).getTime();
										final String objOwner = rs.getString(11);
										final String objType = rs.getString(12);
										final int objState = rs.getInt(13);
										final String extLink = rs.getString(14);
										final boolean trListable = objState == RunnerLinkLoader.STATE_SYSTEM || objState == RunnerLinkLoader.STATE_PUBLISH;
										final boolean trSearchable = objState == RunnerLinkLoader.STATE_ARCHIVE || objState == RunnerLinkLoader.STATE_PUBLISH;
										link.setLoadResult(
												lnkLuid,
												lnkCntId,
												lnkName,
												lnkFolder,
												objId,
												vrId,
												objTitle,
												objCreated,
												objDate,
												objOwner,
												objType,
												objState,
												trListable,
												trSearchable,
												extLink);
									}
								}
							}
						}
					} catch (final Throwable t) {
						for (final LinkData task : map.values()) {
							this.add(task);
						}
						map.clear();
						throw t instanceof SQLException
							? new RuntimeException("QUERY: " + query, t)
							: t;
					} finally {
						for (final LinkData task : map.values()) {
							task.setLoadNotFound();
						}
						map.clear();
					}
				}
			}
		}
		return !this.destroyed;
	}

	@Override
	public final boolean start() {

		this.destroyed = false;
		return true;
	}

	@Override
	public final boolean stop() {

		this.destroyed = true;
		this.queue.switchQueueWaitCancel();
		return false;
	}

	@Override
	public String toString() {

		return this.getClass().getSimpleName() + "(" + this.server + ")";
	}

	@Override
	public final boolean unhandledException(final Throwable t) {

		Report.exception("SERVICE:S3-DB-LINK-LOADER", "Unhandled exception while loading links", t);
		synchronized (this) {
			try {
				// sleep
				Thread.sleep(99L);
				// wait for incoming
				this.wait(399L);
			} catch (final InterruptedException e) {
				return false;
			}
		}
		return !this.destroyed;
	}

	final void add(final LinkData record) {

		if (this.destroyed) {
			throw new IllegalStateException("Link loader is already destroyed!");
		}
		this.queue.offerLast(record);
	}
}
