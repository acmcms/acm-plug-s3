/*
 * Created on 30.06.2004
 */
package ru.myx.xstore.s3.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import ru.myx.ae3.Engine;
import ru.myx.ae3.act.Act;
import ru.myx.ae3.base.Base;
import ru.myx.ae3.base.BaseObject;
import ru.myx.ae3.help.Format;
import ru.myx.ae3.help.Text;
import ru.myx.ae3.report.Report;
import ru.myx.ae3.status.StatusFiller;
import ru.myx.ae3.status.StatusInfo;
import ru.myx.xstore.s3.StorageLevel3;
import ru.myx.xstore.s3.concept.InvalidationEventType;

/** @author myx */
final class RunnerServerPresence implements Runnable, StatusFiller {

	private static final class ImportedInvalidation {

		String evtType;

		String evtGuid;

		long evtDateMin;

		long evtDateMax;

		long evtDate;

		ImportedInvalidation(final String evtType, final String evtGuid, final long evtDateMin, final long evtDateMax) {

			this.evtType = evtType;
			this.evtGuid = evtGuid;
			this.evtDateMin = evtDateMin;
			this.evtDateMax = evtDateMax;
		}

		@Override
		public final String toString() {

			return "(evtType=? AND evtGuid=? AND evtDate<=?)";
		}

		final int fillParameters(final PreparedStatement ps, final int baseIndex) throws SQLException {

			ps.setString(baseIndex + 1, this.evtType);
			ps.setString(baseIndex + 2, this.evtGuid);
			ps.setTimestamp(baseIndex + 3, new Timestamp(this.evtDateMax + 1000L));
			return 3;
		}
	}

	private static final String OWNER = "S3/PRESENCE";

	private static final int LIMIT_BULK_TASKS = 128;

	private static final long PERIOD_CUT_PEER_SERVER = 2L * 60_000L * 60L * 24L;

	private static final long PERIOD_CUT_PEER_CLIENT = 1L * 60_000L * 60L * 24L;

	private static final long PERIOD_UPDATE_PEER = 15L * 60_000L;

	private final StorageLevel3 storage;

	private final boolean client;

	private boolean update = false;

	private long lastUpdate = 0L;

	private final List<ImportedInvalidation> doneInvalidations = new ArrayList<>();

	private int stsEmpty;

	private int stsProcessed;

	private int stsPeerUpdates;

	private boolean destroyed = false;

	RunnerServerPresence(final StorageLevel3 storage, final boolean client) {

		this.storage = storage;
		this.client = client;
	}

	private final void addDoneInvalidation(final String evtType, final String evtGuid, final long evtDateMin, final long evtDateMax) {

		for (final ImportedInvalidation current : this.doneInvalidations) {
			if (current.evtType.equals(evtType) && current.evtGuid.equals(evtGuid)) {
				if (current.evtDateMin > evtDateMin) {
					current.evtDateMin = evtDateMin;
				}
				if (current.evtDateMax < evtDateMax) {
					current.evtDateMax = evtDateMax;
				}
				final long actual = Math.max(System.currentTimeMillis(), current.evtDateMax + 1000L);
				if (current.evtDate < actual) {
					current.evtDate = actual;
				}
				return;
			}
		}
		this.doneInvalidations.add(new ImportedInvalidation(evtType, evtGuid, evtDateMin, evtDateMax));
	}

	@SuppressWarnings("resource")
	@Override
	public final void run() {

		if (this.destroyed) {
			return;
		}
		final Connection conn;
		try {
			conn = this.storage.nextConnection();
			if (conn == null) {
				if (!this.destroyed) {
					Act.later(null, this, 10_000L);
				}
				return;
			}
		} catch (final Throwable t) {
			if (!this.destroyed) {
				Act.later(null, this, 10_000L);
			}
			return;
		}
		boolean anyLoad = false;
		try {
			// do maintenance
			final long time = Engine.fastTime();
			final String identity = this.storage.getIdentity();
			if (this.lastUpdate + RunnerServerPresence.PERIOD_UPDATE_PEER < time) {
				this.stsPeerUpdates++;
				{
					final BaseObject settings = this.storage.getSettingsPrivate();
					settings.baseDefine("lastStart", Base.forDateMillis(time));
					this.storage.commitPrivateSettings();
				}
				final Timestamp dateCurrent = new Timestamp(time);
				final Timestamp dateExpire = new Timestamp(
						time + (this.client
							? RunnerServerPresence.PERIOD_CUT_PEER_CLIENT
							: RunnerServerPresence.PERIOD_CUT_PEER_SERVER));
				final int updated;
				try (final PreparedStatement ps = conn.prepareStatement("UPDATE s3ChangePeer SET peerDate=?, peerExpire=? WHERE peerId=?")) {
					ps.setTimestamp(1, dateCurrent);
					ps.setTimestamp(2, dateExpire);
					ps.setString(3, identity);
					updated = ps.executeUpdate();
					Report.info(RunnerServerPresence.OWNER, "update state (" + updated + ')');
				}
				if (updated == 0) {
					{
						this.storage.setPresenceValidityDate(Engine.fastTime());
						final BaseObject settings = this.storage.getSettingsPrivate();
						settings.baseDefine("presenceValidFrom", Base.forDateMillis(this.storage.getPresenceValidityDate()));
						this.storage.commitPrivateSettings();
					}
					try (final PreparedStatement ps = conn.prepareStatement("INSERT INTO s3ChangePeer(peerDate,peerExpire,peerId) VALUES(?,?,?)")) {
						ps.setTimestamp(1, dateCurrent);
						ps.setTimestamp(2, dateExpire);
						ps.setString(3, identity);
						ps.execute();
					}
					Report.info(RunnerServerPresence.OWNER, "insert state");
				}
				if (!this.client && this.update) {
					{
						final int deleted;
						try (final PreparedStatement ps = conn.prepareStatement("DELETE FROM s3ChangePeer WHERE peerExpire<?")) {
							ps.setTimestamp(1, dateCurrent);
							deleted = ps.executeUpdate();
						}
						if (deleted > 0) {
							Report.info(RunnerServerPresence.OWNER, "delete dead peers (" + deleted + ')');
						}
					}
					{
						final int deleted;
						try (final PreparedStatement ps = conn
								.prepareStatement("DELETE FROM s3ChangeInfo WHERE evtExpire<? OR evtTarget NOT IN (SELECT peerId FROM s3ChangePeer)")) {
							ps.setTimestamp(1, dateCurrent);
							deleted = ps.executeUpdate();
						}
						if (deleted > 0) {
							Report.info(RunnerServerPresence.OWNER, "delete dead events (" + deleted + ')');
						}
					}
				}
				this.lastUpdate = Engine.fastTime();
			}
			// do update
			{
				final boolean update = this.update;
				try {
					try (final PreparedStatement ps = conn.prepareStatement(
							update
								? "SELECT evtType,evtGuid,MIN(evtDate),MAX(evtDate),COUNT(*),CASE WHEN MAX(evtTarget)=? THEN 1 ELSE 0 END FROM s3ChangeInfo WHERE evtTarget IN(?,?) GROUP BY evtType,evtGuid ORDER BY 3 ASC"
								: "SELECT evtType,evtGuid,MIN(evtDate),MAX(evtDate),COUNT(*) FROM s3ChangeInfo WHERE evtTarget=? GROUP BY evtType,evtGuid ORDER BY 3 ASC",
							ResultSet.TYPE_FORWARD_ONLY,
							ResultSet.CONCUR_READ_ONLY)) {
						{
							int index = 0;
							if (update) {
								ps.setString(++index, "$update");
							}
							ps.setString(++index, identity);
							if (update) {
								ps.setString(++index, "$update");
							}
						}
						ps.setMaxRows(RunnerServerPresence.LIMIT_BULK_TASKS);
						try (final ResultSet rs = ps.executeQuery()) {
							if (rs.next()) {
								anyLoad = true;
								do {
									final String evtType = rs.getString(1);
									final String evtGuid = rs.getString(2);
									final long evtDateMin = rs.getTimestamp(3).getTime();
									final long evtDateMax = rs.getTimestamp(4).getTime();
									final int evtCount = rs.getInt(5);
									final boolean evtUpdate = update && rs.getInt(6) == 1;

									Report.info(
											RunnerServerPresence.OWNER,
											"imported update info: cmd=" + evtType + ", guid=" + evtGuid + ", update=" + evtUpdate + ", count=" + evtCount);

									if (evtUpdate) {
										Report.warning(RunnerServerPresence.OWNER, "update events not supported yet!");
									} else {
										final InvalidationEventType type = InvalidationEventType.getEventType(evtType);
										type.invalidateOn(this.storage.getServerInterface(), evtGuid);
										this.addDoneInvalidation(evtType, evtGuid, evtDateMin, evtDateMax);
									}
									this.stsProcessed++;
								} while (rs.next());
							} else {
								this.stsEmpty++;
							}
						}
					}
				} catch (final SQLException e) {
					throw new RuntimeException(e);
				}
				if (!this.doneInvalidations.isEmpty()) {
					try (final PreparedStatement ps = conn.prepareStatement("DELETE FROM s3ChangeInfo WHERE evtTarget=? AND (" + Text.join(this.doneInvalidations, " OR ") + ")")) {
						int index = 0;
						ps.setString(++index, identity);
						for (final ImportedInvalidation current : this.doneInvalidations) {
							index += current.fillParameters(ps, index);
						}
						ps.executeUpdate();
						this.doneInvalidations.clear();
					}
				}
			}
		} catch (final Exception e) {
			Report.exception(RunnerServerPresence.OWNER, "unhandled", e);
		} finally {
			try {
				conn.close();
			} catch (final Throwable t) {
				// ignore
			}
			if (!this.destroyed) {
				Act.later(
						null,
						this,
						anyLoad
							? 2_500L
							: 15_000L);
			}
		}
	}

	@Override
	public final void statusFill(final StatusInfo data) {

		data.put("PRESENCE, bulk task limit", Format.Compact.toDecimal(RunnerServerPresence.LIMIT_BULK_TASKS));
		data.put("PRESENCE, peer updates", Format.Compact.toDecimal(this.stsPeerUpdates));
		data.put("PRESENCE, processed requests", Format.Compact.toDecimal(this.stsProcessed));
		data.put("PRESENCE, empty loops", Format.Compact.toDecimal(this.stsEmpty));
	}

	@Override
	public String toString() {

		return this.getClass().getSimpleName() + "(" + this.storage + ")";
	}

	void setUpdateInterest(final boolean update) {

		this.update = update;
	}

	void start() {

		this.destroyed = false;
		Act.later(null, this, 1500L);
	}

	void stop() {

		this.destroyed = true;
	}
}
