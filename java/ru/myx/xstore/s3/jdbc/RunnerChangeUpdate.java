/*
 * Created on 30.06.2004
 */
package ru.myx.xstore.s3.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import ru.myx.ae1.storage.BaseChange;
import ru.myx.ae1.storage.BaseEntry;
import ru.myx.ae1.storage.ModuleInterface;
import ru.myx.ae1.types.Type;
import ru.myx.ae2.indexing.Indexing;
import ru.myx.ae3.Engine;
import ru.myx.ae3.act.Act;
import ru.myx.ae3.base.Base;
import ru.myx.ae3.base.BaseMap;
import ru.myx.ae3.base.BaseNativeObject;
import ru.myx.ae3.base.BaseObject;
import ru.myx.ae3.control.field.ControlField;
import ru.myx.ae3.control.fieldset.ControlFieldset;
import ru.myx.ae3.help.Convert;
import ru.myx.ae3.help.Create;
import ru.myx.ae3.help.Format;
import ru.myx.ae3.report.Report;
import ru.myx.jdbc.lock.Runner;
import ru.myx.util.EntrySimple;
import ru.myx.xstore.s3.StorageLevel3;
import ru.myx.xstore.s3.concept.InvalidationCollector;
import ru.myx.xstore.s3.concept.InvalidationEventType;
import ru.myx.xstore.s3.concept.LinkData;

/** @author myx */
final class RunnerChangeUpdate implements Runnable, Runner {

	private static final String OWNER = "S3/UPDATE";

	private static final int LIMIT_BULK_TASKS = 32;

	private static final int LIMIT_BULK_UPGRADE = RunnerChangeUpdate.LIMIT_BULK_TASKS * 8;

	private static final long PERIOD_CUT_HISTORY = 30L * 1000L * 60L * 60L * 24L;

	private static final long PERIOD_CUT_RECYCLED = 2L * 1000L * 60L * 60L * 24L * 7L;

	private static final int SEQ_HIGH = 4255397;

	private static final int SEQ_MEDIUM = 2206241;

	private static final Set<String> SYSTEM_KEYS = RunnerChangeUpdate.createSystemKeys();

	private static final void
			createSyncs(final Connection conn, final List<Map.Entry<String, String>> created, final String entryGuid, final String parentGuid, final String taskName)
					throws Exception {

		for (final Map.Entry<String, String> item : created) {
			final String newGuid = item.getKey();
			final String cntLnkId = item.getValue();
			try (final PreparedStatement ps = conn.prepareStatement(
					"INSERT INTO s3TreeSync(lnkSrcId,lnkTgtId) ("
							+ "SELECT ?,? FROM s3TreeSync s LEFT OUTER JOIN s3TreeSync j ON j.lnkSrcId=? AND j.lnkTgtId=? WHERE s.lnkSrcId=? AND s.lnkTgtId=? AND j.lnkSrcId is NULL "
							+ "UNION "
							+ "SELECT ?,? FROM s3TreeSync s LEFT OUTER JOIN s3TreeSync j ON j.lnkSrcId=? AND j.lnkTgtId=? WHERE s.lnkSrcId=? AND s.lnkTgtId=? AND j.lnkSrcId is NULL "
							+ ")")) {
				ps.setString(0x1, entryGuid);
				ps.setString(0x2, newGuid);
				ps.setString(0x3, entryGuid);
				ps.setString(0x4, newGuid);
				ps.setString(0x5, parentGuid);
				ps.setString(0x6, cntLnkId);
				ps.setString(0x7, newGuid);
				ps.setString(0x8, entryGuid);
				ps.setString(0x9, newGuid);
				ps.setString(0xA, entryGuid);
				ps.setString(0xB, cntLnkId);
				ps.setString(0xC, parentGuid);
				final int updateCount = ps.executeUpdate();
				if (updateCount > 0) {
					Report.info(RunnerChangeUpdate.OWNER, updateCount + " syncs created (" + taskName + "), guid=" + entryGuid);
				}
			}
		}
	}

	private static final Set<String> createSystemKeys() {

		final Set<String> result = Create.tempSet();
		result.add("$key");
		result.add("$title");
		result.add("$folder");
		result.add("$type");
		result.add("$state");
		result.add("$owner");
		return result;
	}

	private static final void doMaintainClearDeadAliases(final Connection conn) {

		try {
			final List<String> links = RunnerChangeUpdate.doMaintainClearDeadAliasesGetThem0(conn);
			if (links != null && !links.isEmpty()) {
				for (int i = links.size(); i > 0;) {
					final String alLnkId = links.get(--i);
					final String alId = links.get(--i);
					MatAlias.delete(conn, alId, alLnkId);
				}
			}
		} catch (final RuntimeException e) {
			throw e;
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	private static final List<String> doMaintainClearDeadAliasesGetThem0(final Connection conn) throws SQLException {

		try (final PreparedStatement ps = conn.prepareStatement(
				"SELECT a.alId,a.alLnkId FROM s3Aliases a LEFT OUTER JOIN s3Tree t ON a.alLnkId=t.lnkId LEFT OUTER JOIN s3RecycledTree r ON a.alLnkId=r.lnkId WHERE t.lnkId is NULL AND r.lnkId is NULL",
				ResultSet.TYPE_FORWARD_ONLY,
				ResultSet.CONCUR_READ_ONLY)) {
			try (final ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					final List<String> result = new ArrayList<>();
					do {
						result.add(rs.getString(1));
						result.add(rs.getString(2));
					} while (rs.next());
					return result;
				}
				return null;
			} catch (final SQLException e) {
				throw new RuntimeException(e);
			}
		}
	}

	private static final void doMaintainClearDeadDictionaryWords(final Connection conn) {

		final String q = "DELETE FROM s3Dictionary WHERE code in (SELECT code FROM s3Dictionary LEFT OUTER JOIN s3Indices USING(code) WHERE luid IS NULL)";
		try (final Statement st = conn.createStatement()) {
			st.executeUpdate(q);
		} catch (final SQLException e) {
			final String t = e.getMessage();
			if (t.indexOf("for update in FROM clause") == -1 && t.indexOf("is specified twice, both as a target for") == -1) {
				throw new RuntimeException(e);
			}
			Report.error(RunnerChangeUpdate.OWNER, "MYSQL bug detected, no workaround known, skipping treeSync cleanup, query=" + q);
		} catch (final RuntimeException e) {
			throw e;
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	private static final List<String> doMaintainClearDeadExtraGetThem0(final Connection conn) throws SQLException {

		try (final PreparedStatement ps = conn.prepareStatement(
				"SELECT e.recId FROM s3Extra e LEFT OUTER JOIN s3ExtraLink l ON e.recId=l.recId WHERE e.recDate<? AND l.recId IS NULL GROUP BY e.recId",
				ResultSet.TYPE_FORWARD_ONLY,
				ResultSet.CONCUR_READ_ONLY)) {
			ps.setTimestamp(1, new Timestamp(Engine.fastTime() - 1000L * 60L * 60L));
			try (final ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					final List<String> result = new ArrayList<>();
					do {
						result.add(rs.getString(1));
					} while (rs.next());
					return result;
				}
				return null;
			} catch (final SQLException e) {
				throw new RuntimeException(e);
			}
		}
	}

	private static final void doMaintainClearDeadExtraLinksToExtras(final Connection conn) {

		try {
			final List<String> objects = RunnerChangeUpdate.doMaintainClearDeadExtraLinksToExtrasGetThem0(conn);
			if (objects != null && !objects.isEmpty()) {
				for (final String guid : objects) {
					MatExtra.unlink(conn, guid, null);
				}
			}
		} catch (final RuntimeException e) {
			throw e;
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	private static final List<String> doMaintainClearDeadExtraLinksToExtrasGetThem0(final Connection conn) throws SQLException {

		try (final PreparedStatement ps = conn.prepareStatement(
				"SELECT l.recId FROM s3ExtraLink l LEFT OUTER JOIN s3Extra e ON l.recId=e.recId LEFT OUTER JOIN s3Objects o ON l.objId=o.objId WHERE (o.objCreated is NULL OR o.objCreated<?) AND e.recId is NULL GROUP BY l.recId",
				ResultSet.TYPE_FORWARD_ONLY,
				ResultSet.CONCUR_READ_ONLY)) {
			ps.setTimestamp(1, new Timestamp(Engine.fastTime() - 1000L * 60L * 60L));
			try (final ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					final List<String> result = new ArrayList<>();
					do {
						result.add(rs.getString(1));
					} while (rs.next());
					return result;
				}
				return null;
			} catch (final SQLException e) {
				throw new RuntimeException(e);
			}
		}
	}

	private static final void doMaintainClearDeadExtraLinksToObjects(final Connection conn) {

		try {
			final List<String> objects = RunnerChangeUpdate.doMaintainClearDeadExtraLinksToObjectsGetThem0(conn);
			if (objects != null && !objects.isEmpty()) {
				for (final String guid : objects) {
					MatExtra.unlink(conn, null, guid);
				}
			}
		} catch (final RuntimeException e) {
			throw e;
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	private static final List<String> doMaintainClearDeadExtraLinksToObjectsGetThem0(final Connection conn) throws SQLException {

		try (final PreparedStatement ps = conn.prepareStatement(
				"SELECT l.objId FROM s3ExtraLink l LEFT OUTER JOIN s3Objects o ON l.objId=o.objId LEFT OUTER JOIN s3ObjectHistory h ON l.objId=h.hsId LEFT OUTER JOIN s3ObjectVersions v ON l.objId=v.vrId LEFT OUTER JOIN s3Extra e ON l.recId=e.recId WHERE (e.recDate is NULL OR e.recDate<?) AND o.objId is NULL AND h.hsId is NULL AND v.vrId is NULL GROUP BY l.objId",
				ResultSet.TYPE_FORWARD_ONLY,
				ResultSet.CONCUR_READ_ONLY)) {
			ps.setTimestamp(1, new Timestamp(Engine.fastTime() - 1000L * 60L * 60L));
			try (final ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					final List<String> result = new ArrayList<>();
					do {
						result.add(rs.getString(1));
					} while (rs.next());
					return result;
				}
				return null;
			} catch (final SQLException e) {
				throw new RuntimeException(e);
			}
		}
	}

	private static final List<String> doMaintainClearDeadHistoryGetThem0(final Connection conn) throws SQLException {

		try (final PreparedStatement ps = conn.prepareStatement(
				"SELECT h.objId FROM s3ObjectHistory h LEFT OUTER JOIN s3Objects o ON h.objId=o.objId WHERE o.objId is NULL GROUP BY h.objId",
				ResultSet.TYPE_FORWARD_ONLY,
				ResultSet.CONCUR_READ_ONLY)) {
			try (final ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					final List<String> result = new ArrayList<>();
					do {
						result.add(rs.getString(1));
					} while (rs.next());
					return result;
				}
				return null;
			} catch (final SQLException e) {
				throw new RuntimeException(e);
			}
		}
	}

	private static final List<Object> doMaintainClearDeadLinksGetThem0(final Connection conn) throws SQLException {

		try (final PreparedStatement ps = conn.prepareStatement(
				"SELECT t1.lnkLuid,t1.objId FROM s3Tree t1 LEFT OUTER JOIN s3Tree t2 ON t1.cntLnkId=t2.lnkId LEFT OUTER JOIN s3Objects o ON t1.objId=o.objId WHERE (t2.lnkId is NULL OR o.objId is NULL) AND t1.cntLnkId != '*'",
				ResultSet.TYPE_FORWARD_ONLY,
				ResultSet.CONCUR_READ_ONLY)) {
			try (final ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					final List<Object> result = new ArrayList<>();
					do {
						result.add(Integer.valueOf(rs.getInt(1)));
						result.add(rs.getString(2));
					} while (rs.next());
					return result;
				}
				return null;
			} catch (final SQLException e) {
				throw new RuntimeException(e);
			}
		}
	}

	private static final void doMaintainClearDeadObjectHistories(final Connection conn) {

		try {
			try (final PreparedStatement ps = conn.prepareStatement("DELETE FROM s3ObjectHistory WHERE hsDate<?")) {
				ps.setTimestamp(1, new Timestamp(Engine.fastTime() - RunnerChangeUpdate.PERIOD_CUT_HISTORY));
				ps.execute();
			}
			final List<String> objects = RunnerChangeUpdate.doMaintainClearDeadHistoryGetThem0(conn);
			if (objects != null && !objects.isEmpty()) {
				for (final String guid : objects) {
					MatHistory.clear(conn, guid);
				}
			}
		} catch (final RuntimeException e) {
			throw e;
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	private static final void doMaintainClearDeadObjects(final Connection conn) {

		try {
			final List<String> objects = RunnerChangeUpdate.doMaintainClearDeadObjectsGetThem0(conn);
			if (objects != null && !objects.isEmpty()) {
				for (final String guid : objects) {
					MatExtra.unlink(conn, null, guid);
					MatData.delete(conn, guid);
					MatHistory.clear(conn, guid);
				}
			}
		} catch (final RuntimeException e) {
			throw e;
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	private static final List<String> doMaintainClearDeadObjectsGetThem0(final Connection conn) throws SQLException {

		try (final PreparedStatement ps = conn.prepareStatement(
				"SELECT o.objId FROM s3Objects o LEFT OUTER JOIN s3Tree t ON o.objId=t.objId LEFT OUTER JOIN s3RecycledTree r ON o.objId=r.objId WHERE o.objCreated<? AND t.objId is NULL AND r.objId is NULL",
				ResultSet.TYPE_FORWARD_ONLY,
				ResultSet.CONCUR_READ_ONLY)) {
			ps.setTimestamp(1, new Timestamp(Engine.fastTime() - 12L * 1000L * 60L * 60L));
			try (final ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					final List<String> result = new ArrayList<>();
					do {
						result.add(rs.getString(1));
					} while (rs.next());
					return result;
				}
				return null;
			} catch (final SQLException e) {
				throw new RuntimeException(e);
			}
		}
	}

	private static final void doMaintainClearDeadObjectVersions(final Connection conn) {

		try {
			final List<String> objects = RunnerChangeUpdate.doMaintainClearDeadVersionsGetThem0(conn);
			if (objects != null && !objects.isEmpty()) {
				for (final String guid : objects) {
					MatVersion.clear(conn, guid);
				}
			}
		} catch (final RuntimeException e) {
			throw e;
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	private static final void doMaintainClearDeadRecycled(final Connection conn) {

		try {
			try (final PreparedStatement ps = conn.prepareStatement("DELETE FROM s3Recycled WHERE delDate<?")) {
				ps.setTimestamp(1, new Timestamp(Engine.fastTime() - RunnerChangeUpdate.PERIOD_CUT_RECYCLED));
				ps.execute();
			}
			final List<String> links = RunnerChangeUpdate.doMaintainClearDeadRecycledGetThem0(conn);
			if (links != null && !links.isEmpty()) {
				for (final String guid : links) {
					MatRecycled.clearRecycled(conn, guid);
				}
			}
		} catch (final RuntimeException e) {
			throw e;
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	private static final List<String> doMaintainClearDeadRecycledGetThem0(final Connection conn) throws SQLException {

		try (final PreparedStatement ps = conn.prepareStatement(
				"SELECT t.delId FROM s3RecycledTree t LEFT OUTER JOIN s3Recycled r ON t.delId=r.delRootId WHERE r.delRootId is NULL GROUP BY t.delId",
				ResultSet.TYPE_FORWARD_ONLY,
				ResultSet.CONCUR_READ_ONLY)) {
			try (final ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					final List<String> result = new ArrayList<>();
					do {
						result.add(rs.getString(1));
					} while (rs.next());
					return result;
				}
				return null;
			} catch (final SQLException e) {
				throw new RuntimeException(e);
			}
		}
	}

	private static final void doMaintainClearDeadSynchronizations(final Connection conn) {

		final String q1 = "DELETE FROM s3TreeSync WHERE lnkSrcId in (SELECT s.lnkSrcId FROM s3Tree t RIGHT OUTER JOIN s3TreeSync s ON s.lnkSrcId=t.lnkId WHERE t.lnkId is NULL)";
		final String q2 = "DELETE FROM s3TreeSync WHERE lnkTgtId in (SELECT s.lnkTgtId FROM s3Tree t RIGHT OUTER JOIN s3TreeSync s ON s.lnkTgtId=t.lnkId WHERE t.lnkId is NULL)";
		try (final Statement st = conn.createStatement()) {
			st.executeUpdate(q1);
			st.executeUpdate(q2);
		} catch (final SQLException e) {
			final String t = e.getMessage();
			if (t.indexOf("for update in FROM clause") == -1 && t.indexOf("is specified twice, both as a target for") == -1) {
				throw new RuntimeException(e);
			}
			Report.error(RunnerChangeUpdate.OWNER, "MYSQL bug detected, no workaround known, skipping treeSync cleanup, query=" + q1);
		} catch (final RuntimeException e) {
			throw e;
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	private static final List<String> doMaintainClearDeadVersionsGetThem0(final Connection conn) throws SQLException {

		try (final PreparedStatement ps = conn.prepareStatement(
				"SELECT v.objId FROM s3ObjectVersions v LEFT OUTER JOIN s3Objects o ON v.objId=o.objId WHERE o.objId is NULL GROUP BY v.objId",
				ResultSet.TYPE_FORWARD_ONLY,
				ResultSet.CONCUR_READ_ONLY)) {
			try (final ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					final List<String> result = new ArrayList<>();
					do {
						result.add(rs.getString(1));
					} while (rs.next());
					return result;
				}
				return null;
			} catch (final SQLException e) {
				throw new RuntimeException(e);
			}
		}
	}

	private static final void doMaintainFix(final Connection conn) {

		try (final PreparedStatement ps = conn.prepareStatement("UPDATE s3Tree SET cntLnkId=? WHERE cntLnkId!=? AND lnkId=?")) {
			ps.setString(1, "*");
			ps.setString(2, "*");
			ps.setString(3, "$$ROOT_ENTRY");
			if (ps.executeUpdate() > 0) {
				Report.warning(RunnerChangeUpdate.OWNER, "ROOT-FIX happened!");
			}
		} catch (final RuntimeException e) {
			throw e;
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	private static final List<Object> doMaintainFixIndicesGetThem0(final Connection conn) throws SQLException {

		try (final PreparedStatement ps = conn.prepareStatement(
				"SELECT t.lnkId,t.lnkLuid FROM s3Tree t LEFT OUTER JOIN s3Indexed i ON t.lnkLuid=i.luid LEFT OUTER JOIN s3Objects o ON t.objId=o.objId WHERE i.luid is NULL AND o.objDate<?",
				ResultSet.TYPE_FORWARD_ONLY,
				ResultSet.CONCUR_READ_ONLY)) {
			ps.setTimestamp(1, new Timestamp(Engine.fastTime() - 1000L * 60L * 60L));
			try (final ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					final List<Object> result = new ArrayList<>();
					do {
						result.add(rs.getString(1));
						result.add(Integer.valueOf(rs.getInt(2)));
					} while (rs.next());
					return result;
				}
				return null;
			} catch (final SQLException e) {
				throw new RuntimeException(e);
			}
		}
	}

	private static final void eventDone(final Connection conn, final String evtId) throws SQLException {

		try (final PreparedStatement ps = conn.prepareStatement("DELETE FROM s3ChangeQueue WHERE evtId=?")) {
			ps.setString(1, evtId);
			ps.execute();
		}
	}

	private final ServerJdbc server;

	private final StorageLevel3 storage;

	private final Indexing indexing;

	private final int indexingVersion;

	private final InvalidationCollector invalidations = new InvalidationCollector();

	private final RunnerServerPresence serverPresence;

	private boolean destroyed = false;

	private boolean failedLastEventIndexedCreate = false;

	RunnerChangeUpdate(final StorageLevel3 storage, final ServerJdbc server, final RunnerServerPresence serverPresence, final Indexing indexing) {

		this.storage = storage;
		this.server = server;
		this.serverPresence = serverPresence;
		this.indexing = indexing;
		this.indexingVersion = indexing.getVersion();
	}

	private final boolean analyzeCreateSyncs(final Connection conn,
			final int entryLuid,
			final String entryGuid,
			final String entryKey,
			final String parentGuid,
			final String updateName,
			final String taskName) throws Exception {

		boolean heavyLoad = false;
		final List<Map.Entry<String, String>> list = new ArrayList<>();
		// Do synchronization
		try (final PreparedStatement ps = conn.prepareStatement(
				"SELECT s.lnkTgtId FROM s3TreeSync s LEFT OUTER JOIN s3Tree t ON t.cntLnkId=s.lnkTgtId AND t.lnkName=? WHERE s.lnkSrcId=? AND t.lnkId is NULL",
				ResultSet.TYPE_FORWARD_ONLY,
				ResultSet.CONCUR_READ_ONLY)) {
			ps.setString(1, entryKey);
			ps.setString(2, parentGuid);
			try (final ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					list.add(new EntrySimple<>(Engine.createGuid(), rs.getString(1)));
				}
			}
		}
		// Create links
		if (!list.isEmpty()) {
			Report.event(
					RunnerChangeUpdate.OWNER,
					"ANALYZE_CREATE_LINKS",
					"pending: count=" + list.size() + ", entryGuid=" + entryGuid + ", entryKey=" + entryKey + ", parentGuid=" + parentGuid);
			for (final Map.Entry<String, String> current : list) {
				final String cntLnkId = current.getValue();
				final BaseEntry<?> target = this.server.getStorage().getInterface().getByGuid(cntLnkId);
				if (target != null) {
					final String newGuid = current.getKey();
					try (final PreparedStatement ps = conn.prepareStatement(
							"INSERT INTO s3Tree(lnkId,cntLnkId,lnkName,objId,lnkFolder) SELECT ?,?,t.lnkName,t.objId,t.lnkFolder FROM s3Tree t WHERE t.lnkLuid=" + entryLuid)) {
						ps.setString(1, newGuid);
						ps.setString(2, cntLnkId);
						ps.executeUpdate();
						MatChange.serialize(this.server, conn, 0, updateName, newGuid, -1);
						this.invalidations.add(InvalidationEventType.UTREE, cntLnkId);
						heavyLoad = true;
					}
					try (final PreparedStatement ps = conn.prepareStatement("SELECT objId FROM s3Tree WHERE lnkId=?")) {
						ps.setString(1, newGuid);
						try (final ResultSet rs = ps.executeQuery()) {
							while (rs.next()) {
								this.invalidations.add(InvalidationEventType.UIDEN, rs.getString(1));
							}
						}
					}
				}
			}
			RunnerChangeUpdate.createSyncs(conn, list, entryGuid, parentGuid, taskName);
		}
		return heavyLoad;
	}

	private final void doClean(final Connection conn, final Map<String, Object> task) throws Exception {

		final String guid = Convert.MapEntry.toString(task, "evtCmdGuid", "").trim();
		final int luid = Convert.MapEntry.toInt(task, "evtCmdLuid", -1);
		if (luid != -1) {
			this.doCleanIndices(conn, luid);
		}
		this.doUpdateObject(conn, guid);
	}

	private final void doCleanAll(final Connection conn, final Map<String, Object> task) throws Exception {

		final String guid = Convert.MapEntry.toString(task, "evtCmdGuid", "").trim();
		final List<Object> linkData = new ArrayList<>();
		try (final PreparedStatement ps = conn
				.prepareStatement("SELECT t.lnkId,t.cntLnkId,t.lnkLuid FROM s3Tree t WHERE t.objId=?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
			ps.setString(1, guid);
			try (final ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					linkData.add(rs.getString(1));
					linkData.add(rs.getString(2));
					linkData.add(Integer.valueOf(rs.getInt(3)));
				}
			}
		}
		for (int i = linkData.size(); i > 0;) {
			final int luid = ((Integer) linkData.get(--i)).intValue();
			final String cntId = (String) linkData.get(--i);
			final String lnkId = (String) linkData.get(--i);
			this.doCleanIndices(conn, luid);
			MatLink.unlink(conn, lnkId);
			this.invalidations.add(InvalidationEventType.DLINK, lnkId);
			this.invalidations.add(InvalidationEventType.UTREE, cntId);
		}
		this.doUpdateObject(conn, guid);
	}

	private final void doCleanIndices(final Connection conn, final int lnkLuid) throws Exception {

		Report.event(RunnerChangeUpdate.OWNER, "CLEANING_INDEX", "luid=" + lnkLuid);
		this.indexing.doDelete(conn, lnkLuid);
		try (final Statement st = conn.createStatement()) {
			st.execute("DELETE FROM s3Indexed WHERE luid=" + lnkLuid);
		}
	}

	private boolean doCleanStart(final Connection conn, final Map<String, Object> task) throws Exception {

		final String lnkId = Convert.MapEntry.toString(task, "evtCmdGuid", "").trim();
		final List<Object> items = new ArrayList<>();
		this.fillTree(conn, items, lnkId);
		for (int i = items.size() - 1, sequence = 0; i >= 0; i--, sequence++) {
			final String object = (String) items.get(i);
			final Integer integer = (Integer) items.get(--i);
			MatChange.serialize(this.server, conn, sequence, "delete-item", object, integer.intValue());
		}
		return items.size() > 0;
	}

	private final boolean doCreateGlobal(final Connection conn, final Map<String, Object> task) throws Exception {

		final String guid = Convert.MapEntry.toString(task, "evtCmdGuid", "").trim();
		final LinkData entry = MatLink.materializeActual(conn, guid);
		if (entry != null) {
			final int luid = entry.lnkLuid;
			Report.event(RunnerChangeUpdate.OWNER, "INDEXING", "create/global, guid=" + guid + ", luid=" + luid);
			this.doIndex(conn, entry, luid);
			this.eventIndexed(conn, luid, true);
			final LinkData parent = MatLink.materializeActual(conn, entry.lnkCntId);
			if (parent != null) {
				return this.analyzeCreateSyncs(conn, luid, guid, entry.lnkName, entry.lnkCntId, "create-global", "create-global");
			}
		}
		return false;
	}

	private final boolean doCreateLocal(final Connection conn, final Map<String, Object> task) throws Exception {

		final String guid = Convert.MapEntry.toString(task, "evtCmdGuid", "").trim();
		final LinkData entry = MatLink.materializeActual(conn, guid);
		if (entry != null) {
			final int luid = entry.lnkLuid;
			Report.event(RunnerChangeUpdate.OWNER, "INDEXING", "create/local, guid=" + guid + ", luid=" + luid);
			this.doIndex(conn, entry, luid);
			this.eventIndexed(conn, luid, true);
		}
		return false;
	}

	private void doDeleteItem(final Connection conn, final Map<String, Object> task) throws Exception {

		final String objId = Convert.MapEntry.toString(task, "evtCmdGuid", "").trim();
		final int lnkLuid = Convert.MapEntry.toInt(task, "evtCmdLuid", -1);
		MatAlias.deleteOrMove(conn, lnkLuid);
		this.doCleanIndices(conn, lnkLuid);
		MatLink.unlink(conn, lnkLuid);
		this.doUpdateObject(conn, objId);
	}

	private final void doIndex(final Connection conn, final LinkData entry, final int luid) throws Exception {

		{
			final Set<String> hierarchy = Create.tempSet();
			final StringBuilder parents = new StringBuilder();
			for (LinkData current = entry;; current = this.storage.getServerInterface().getLink(current.lnkCntId)) {
				if (current == null) {
					Report.warning(RunnerChangeUpdate.OWNER, "Lost item detected! parents=" + parents);
					this.makeLost(conn, entry, true);
					break;
				}
				final String parent = current.lnkCntId;
				if ("*".equals(parent)) {
					final Type<?> type = this.storage.getServer().getTypes().getType(entry.objType);
					final ControlFieldset<?> fieldset = type == null
						? null
						: type.getFieldsetLoad();
					final Set<String> excludeFields;
					if (fieldset == null) {
						excludeFields = Collections.emptySet();
					} else {
						final Set<String> exclude = Create.tempSet();
						final int length = fieldset.size();
						for (int i = 0; i < length; ++i) {
							final ControlField field = fieldset.get(i);
							if (!Convert.MapEntry.toBoolean(field.getAttributes(), "indexing", true)) {
								field.fillFields(exclude);
							}
						}
						if (exclude.isEmpty()) {
							excludeFields = Collections.emptySet();
						} else {
							excludeFields = exclude;
						}
					}
					final BaseMap data = new BaseNativeObject(null);
					data.baseDefineImportAllEnumerable(entry.getDataReal(this.storage, conn));
					if (!excludeFields.isEmpty()) {
						for (final String key : excludeFields) {
							data.baseDelete(key);
						}
					}
					data.baseDefine("$key", entry.lnkName);
					data.baseDefine("$title", entry.objTitle);
					data.baseDefine("$folder", entry.lnkFolder);
					data.baseDefine("$type", entry.objType);
					data.baseDefine("$state", entry.objState);
					data.baseDefine("$owner", entry.objOwner);
					final boolean fullText;
					{
						if (fieldset == null) {
							fullText = false;
						} else {
							if (fieldset.getField("KEYWORDS") == null) {
								fullText = false;
							} else {
								fullText = true;
							}
						}
					}
					this.indexing.doIndex(conn, entry.lnkCntId, hierarchy, entry.objState, RunnerChangeUpdate.SYSTEM_KEYS, data, fullText, luid);
					break;
				}
				parents.append(parent).append(' ');
				if (!hierarchy.add(parent)) {
					Report.warning(RunnerChangeUpdate.OWNER, "Recursion detected! parents=" + parents);
					this.makeLost(conn, current, false);
					break;
				}
			}
		}
	}

	private void doMaintainClearDead(final Connection conn, final BaseObject settings) {

		final int lastVersion = Convert.MapEntry.toInt(settings, "runnerVersion", 0);
		final long nextClean = Convert.MapEntry.toLong(settings, "deadCleanupDate", 0L);
		if (lastVersion < this.getVersion() || nextClean < Engine.fastTime()) {
			settings.baseDefine("deadCleanupDate", Base.forDateMillis(Engine.fastTime() + 1000L * 60L * 60L * 24L));
			try {
				conn.setAutoCommit(false);
				try {
					RunnerChangeUpdate.doMaintainFix(conn);
					conn.commit();
				} catch (final Throwable t) {
					try {
						conn.rollback();
					} catch (final Throwable tt) {
						// ignore
					}
					Report.exception(RunnerChangeUpdate.OWNER, "Unexpected exception while collecting garbage in storage", t);
					settings.baseDefine("deadCleanupDate", Base.forDateMillis(Engine.fastTime() + 30L * 1000L * 60L));
				}
				try {
					RunnerChangeUpdate.doMaintainClearDeadDictionaryWords(conn);
					conn.commit();
				} catch (final Throwable t) {
					try {
						conn.rollback();
					} catch (final Throwable tt) {
						// ignore
					}
					Report.exception(RunnerChangeUpdate.OWNER, "Unexpected exception while collecting garbage in storage", t);
					settings.baseDefine("deadCleanupDate", Base.forDateMillis(Engine.fastTime() + 30L * 1000L * 60L));
				}
				try {
					RunnerChangeUpdate.doMaintainClearDeadSynchronizations(conn);
					conn.commit();
				} catch (final Throwable t) {
					try {
						conn.rollback();
					} catch (final Throwable tt) {
						// ignore
					}
					Report.exception(RunnerChangeUpdate.OWNER, "Unexpected exception while collecting garbage in storage", t);
					settings.baseDefine("deadCleanupDate", Base.forDateMillis(Engine.fastTime() + 30L * 1000L * 60L));
				}
				try {
					this.doMaintainClearDeadLinks(conn);
					conn.commit();
				} catch (final Throwable t) {
					try {
						conn.rollback();
					} catch (final Throwable tt) {
						// ignore
					}
					Report.exception(RunnerChangeUpdate.OWNER, "Unexpected exception while collecting garbage in storage", t);
					settings.baseDefine("deadCleanupDate", Base.forDateMillis(Engine.fastTime() + 30L * 1000L * 60L));
				}
				try {
					RunnerChangeUpdate.doMaintainClearDeadRecycled(conn);
					conn.commit();
				} catch (final Throwable t) {
					try {
						conn.rollback();
					} catch (final Throwable tt) {
						// ignore
					}
					Report.exception(RunnerChangeUpdate.OWNER, "Unexpected exception while collecting garbage in storage", t);
					settings.baseDefine("deadCleanupDate", Base.forDateMillis(Engine.fastTime() + 30L * 1000L * 60L));
				}
				try {
					RunnerChangeUpdate.doMaintainClearDeadAliases(conn);
					conn.commit();
				} catch (final Throwable t) {
					try {
						conn.rollback();
					} catch (final Throwable tt) {
						// ignore
					}
					Report.exception(RunnerChangeUpdate.OWNER, "Unexpected exception while collecting garbage in storage", t);
					settings.baseDefine("deadCleanupDate", Base.forDateMillis(Engine.fastTime() + 30L * 1000L * 60L));
				}
				try {
					RunnerChangeUpdate.doMaintainClearDeadObjects(conn);
					conn.commit();
				} catch (final Throwable t) {
					try {
						conn.rollback();
					} catch (final Throwable tt) {
						// ignore
					}
					Report.exception(RunnerChangeUpdate.OWNER, "Unexpected exception while collecting garbage in storage", t);
					settings.baseDefine("deadCleanupDate", Base.forDateMillis(Engine.fastTime() + 30L * 1000L * 60L));
				}
				try {
					RunnerChangeUpdate.doMaintainClearDeadObjectHistories(conn);
					conn.commit();
				} catch (final Throwable t) {
					try {
						conn.rollback();
					} catch (final Throwable tt) {
						// ignore
					}
					Report.exception(RunnerChangeUpdate.OWNER, "Unexpected exception while collecting garbage in storage", t);
					settings.baseDefine("deadCleanupDate", Base.forDateMillis(Engine.fastTime() + 30L * 1000L * 60L));
				}
				try {
					RunnerChangeUpdate.doMaintainClearDeadObjectVersions(conn);
					conn.commit();
				} catch (final Throwable t) {
					try {
						conn.rollback();
					} catch (final Throwable tt) {
						// ignore
					}
					Report.exception(RunnerChangeUpdate.OWNER, "Unexpected exception while collecting garbage in storage", t);
					settings.baseDefine("deadCleanupDate", Base.forDateMillis(Engine.fastTime() + 30L * 1000L * 60L));
				}
				try {
					RunnerChangeUpdate.doMaintainClearDeadExtraLinksToObjects(conn);
					conn.commit();
				} catch (final Throwable t) {
					try {
						conn.rollback();
					} catch (final Throwable tt) {
						// ignore
					}
					Report.exception(RunnerChangeUpdate.OWNER, "Unexpected exception while collecting garbage in storage", t);
					settings.baseDefine("deadCleanupDate", Base.forDateMillis(Engine.fastTime() + 30L * 1000L * 60L));
				}
				try {
					RunnerChangeUpdate.doMaintainClearDeadExtraLinksToExtras(conn);
					conn.commit();
				} catch (final Throwable t) {
					try {
						conn.rollback();
					} catch (final Throwable tt) {
						// ignore
					}
					Report.exception(RunnerChangeUpdate.OWNER, "Unexpected exception while collecting garbage in storage", t);
					settings.baseDefine("deadCleanupDate", Base.forDateMillis(Engine.fastTime() + 30L * 1000L * 60L));
				}
				try {
					this.doMaintainClearDeadExtra(conn);
					conn.commit();
				} catch (final Throwable t) {
					try {
						conn.rollback();
					} catch (final Throwable tt) {
						// ignore
					}
					Report.exception(RunnerChangeUpdate.OWNER, "Unexpected exception while collecting garbage in storage", t);
					settings.baseDefine("deadCleanupDate", Base.forDateMillis(Engine.fastTime() + 30L * 1000L * 60L));
				}
			} catch (final Throwable t) {
				try {
					conn.rollback();
				} catch (final Throwable tt) {
					// ignore
				}
				Report.exception(RunnerChangeUpdate.OWNER, "Unexpected exception while collecting garbage in storage", t);
				settings.baseDefine("deadCleanupDate", Base.forDateMillis(Engine.fastTime() + 30L * 1000L * 60L));
			} finally {
				try {
					conn.setAutoCommit(true);
				} catch (final Throwable t) {
					// ignore
				}
			}
			settings.baseDefine("runnerVersion", this.getVersion());
			this.storage.commitProtectedSettings();
		}
	}

	private void doMaintainClearDeadExtra(final Connection conn) {

		try {
			final List<String> objects = RunnerChangeUpdate.doMaintainClearDeadExtraGetThem0(conn);
			if (objects != null && !objects.isEmpty()) {
				Report.event(RunnerChangeUpdate.OWNER, "CLEAR-DEAD-EXTRA", "deleting " + objects.size() + " dead extras");
				for (final String guid : objects) {
					MatExtra.purge(conn, guid);
					this.invalidations.add(InvalidationEventType.DEXTR, guid);
				}
			}
		} catch (final RuntimeException e) {
			throw e;
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	private void doMaintainClearDeadLinks(final Connection conn) {

		try {
			final List<Object> links = RunnerChangeUpdate.doMaintainClearDeadLinksGetThem0(conn);
			if (links != null && !links.isEmpty()) {
				Report.event(RunnerChangeUpdate.OWNER, "CLEAR-DEAD-LINKS", "deleting " + links.size() + " dead links");
				for (int i = links.size(); i > 0;) {
					final String guid = (String) links.get(--i);
					final int luid = ((Integer) links.get(--i)).intValue();
					MatLink.unlink(conn, luid);
					MatChange.serialize(this.server, conn, 0, "clean", guid, luid);
				}
			}
		} catch (final RuntimeException e) {
			throw e;
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	private void doMaintainIndexing(final Connection conn, final BaseObject settings) {

		final int indexingVersion = Convert.MapEntry.toInt(settings, "indexingVersion", 0);
		if (indexingVersion != this.indexingVersion) {
			try {
				MatChange.serialize(this.server, conn, RunnerChangeUpdate.SEQ_HIGH, "upgrade-index", "*", this.indexingVersion);
			} catch (final SQLException e) {
				throw new RuntimeException(e);
			}
			settings.baseDefine("indexingVersion", this.indexingVersion);
			this.storage.commitProtectedSettings();
		}
		final long nextClean = Convert.MapEntry.toLong(settings, "indexCheckDate", 0L);
		if (nextClean < Engine.fastTime()) {
			try {
				try {
					final List<Object> links = RunnerChangeUpdate.doMaintainFixIndicesGetThem0(conn);
					if (links != null && !links.isEmpty()) {
						for (int i = links.size(); i > 0;) {
							final int luid = ((Integer) links.get(--i)).intValue();
							final String guid = (String) links.get(--i);
							MatChange.serialize(this.server, conn, 0, "update", guid, luid);
						}
					}
				} catch (final RuntimeException e) {
					throw e;
				}
				settings.baseDefine("indexCheckDate", Base.forDateMillis(Engine.fastTime() + 1000L * 60L * 60L * 24L));
				this.storage.commitProtectedSettings();
			} catch (final SQLException e) {
				settings.baseDefine("indexCheckDate", Base.forDateMillis(Engine.fastTime() + 1000L * 60L * 60L * 24L));
				this.storage.commitProtectedSettings();
				throw new RuntimeException(e);
			}
		}
	}

	private void doMaintainVersionUpdate(final Connection conn, final BaseObject settings) {

		final int systemVersion = Convert.MapEntry.toInt(settings, "systemVersion", 0);
		if (systemVersion != this.indexingVersion) {
			try {
				if (systemVersion < 24) {
					Report.info(RunnerChangeUpdate.OWNER, "Version update routine 24 start");
					int updated = 0;
					try (final Statement statement = conn.createStatement()) {
						updated += statement.executeUpdate(
								"INSERT INTO s3TreeSync(lnkSrcId,lnkTgtId) SELECT t1.lnkId,t2.lnkId FROM s3Tree t1 INNER JOIN s3Tree t2 ON t1.objId=t2.objId LEFT OUTER JOIN s3TreeSync s ON s.lnkSrcId=t1.lnkId AND s.lnkTgtId=t2.lnkId WHERE t1.lnkLuid!=t2.lnkLuid AND s.lnkSrcId IS NULL");
						updated += statement.executeUpdate(
								"INSERT INTO s3TreeSync(lnkTgtId,lnkSrcId) SELECT t1.lnkId,t2.lnkId FROM s3Tree t1 INNER JOIN s3Tree t2 ON t1.objId=t2.objId LEFT OUTER JOIN s3TreeSync s ON s.lnkTgtId=t1.lnkId AND s.lnkSrcId=t2.lnkId WHERE t1.lnkLuid!=t2.lnkLuid AND s.lnkTgtId IS NULL");
					}
					Report.info(RunnerChangeUpdate.OWNER, "Version update routine 24 done, updated=" + updated);
				}
				if (systemVersion < 35) {
					Report.info(RunnerChangeUpdate.OWNER, "Version update routine 35 start");
					final Set<String> types = Create.tempSet();
					{
						try (final PreparedStatement ps = conn
								.prepareStatement("SELECT recType FROM s3Extra GROUP BY recType", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
							try (final ResultSet rs = ps.executeQuery()) {
								while (rs.next()) {
									final String type = rs.getString(1);
									if (type.startsWith("try;") || type.startsWith("gzip;") || type.startsWith("plain;")) {
										continue;
									}
									types.add(type);
								}
							}
						}
					}
					int updated = 0;
					for (final String type : types) {
						try (final PreparedStatement ps = conn
								.prepareStatement("SELECT recId FROM s3Extra WHERE recType=?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
							ps.setString(1, type);
							try (final ResultSet rs = ps.executeQuery()) {
								while (rs.next()) {
									this.server.updateExtra(rs.getString(1));
									updated++;
								}
							}
						}
					}
					Report.info(RunnerChangeUpdate.OWNER, "Version update routine 35 done, types=" + types.size() + ", records=" + updated);
				}
			} catch (final SQLException e) {
				throw new RuntimeException(e);
			}
			settings.baseDefine("systemVersion", this.getVersion());
			this.storage.commitProtectedSettings();
		}
	}

	private final void doRecycleAll(final Connection conn, final Map<String, Object> task) throws Exception {

		final String guid = Convert.MapEntry.toString(task, "evtCmdGuid", "").trim();
		final List<Object> linkData = new ArrayList<>();
		try (final PreparedStatement ps = conn
				.prepareStatement("SELECT t.lnkId,t.lnkLuid FROM s3Tree t WHERE t.objId=?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
			ps.setString(1, guid);
			try (final ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					linkData.add(rs.getString(1));
					linkData.add(Integer.valueOf(rs.getInt(2)));
				}
			}
		}
		for (int i = linkData.size(); i > 0;) {
			final int luid = ((Integer) linkData.get(--i)).intValue();
			final String lnkId = (String) linkData.get(--i);
			try {
				MatLink.recycle(conn, lnkId);
				MatChange.serialize(this.server, conn, 0, "recycle-start", lnkId, luid);
			} catch (final SQLException e) {
				Report.exception(RunnerChangeUpdate.OWNER, "sql error while starting recycler process for link '" + lnkId + "', skipping", e);
			}
		}
	}

	private void doRecycleFinish(final Connection conn, final Map<String, Object> task) throws Exception {

		final String delId = Convert.MapEntry.toString(task, "evtCmdGuid", "").trim();
		final int lnkLuid = Convert.MapEntry.toInt(task, "evtCmdLuid", -1);
		this.doCleanIndices(conn, lnkLuid);
		MatLink.unlink(conn, delId);
	}

	private void doRecycleItem(final Connection conn, final Map<String, Object> task) throws Exception {

		final String delId = Convert.MapEntry.toString(task, "evtCmdGuid", "").trim();
		final int lnkLuid = Convert.MapEntry.toInt(task, "evtCmdLuid", -1);
		this.doCleanIndices(conn, lnkLuid);
		MatRecycled.recycleLink(conn, delId, lnkLuid);
	}

	private boolean doRecycleStart(final Connection conn, final Map<String, Object> task) throws Exception {

		final String delId = Convert.MapEntry.toString(task, "evtCmdGuid", "").trim();
		final int lnkLuid = Convert.MapEntry.toInt(task, "evtCmdLuid", -1);
		final List<Object> items = new ArrayList<>();
		this.fillTree(conn, items, lnkLuid);
		if (!items.isEmpty()) {
			for (int i = items.size() - 1, sequence = 0; i >= 0; i--, sequence++) {
				final Integer integer = (Integer) items.get(--i);
				MatChange.serialize(this.server, conn, sequence, "recycle-item", delId, integer.intValue());
			}
			MatChange.serialize(this.server, conn, RunnerChangeUpdate.SEQ_HIGH, "recycle-finish", delId, lnkLuid);
			return items.size() >= RunnerChangeUpdate.LIMIT_BULK_TASKS;
		}
		this.doCleanIndices(conn, lnkLuid);
		MatLink.unlink(conn, delId);
		return false;
	}

	private final boolean doResync(final Connection conn, final Map<String, Object> task) throws Exception {

		final String guid = Convert.MapEntry.toString(task, "evtCmdGuid", "").trim();
		final LinkData entry = MatLink.materializeActual(conn, guid);
		if (entry != null) {
			final int luid = entry.lnkLuid;
			final LinkData parent = this.storage.getServerInterface().getLink(entry.lnkCntId);
			if (parent != null) {
				return this.analyzeCreateSyncs(conn, luid, guid, entry.lnkName, entry.lnkCntId, "create", "resync");
			}
		}
		return false;
	}

	private final void doUpdate(final Connection conn, final Map<String, Object> task) throws Exception {

		final String guid = Convert.MapEntry.toString(task, "evtCmdGuid", "").trim();
		final int luid = Convert.MapEntry.toInt(task, "evtCmdLuid", -1);
		final long date = Convert.MapEntry.toLong(task, "evtDate", 0L);
		this.doUpdate(conn, guid, luid, date);
	}

	private final void doUpdate(final Connection conn, final String lnkId, final int lnkLuid, final long date) throws Exception {

		if (lnkLuid != -1 && date > 0L) {
			try (final PreparedStatement ps = conn
					.prepareStatement("SELECT lnkIndexed FROM s3Indexed WHERE luid=" + lnkLuid, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
				try (final ResultSet rs = ps.executeQuery()) {
					if (rs.next()) {
						final Date indexed = rs.getDate(1);
						if (indexed != null) {
							if (indexed.getTime() > date) {
								Report.event(
										RunnerChangeUpdate.OWNER,
										"INDEXING",
										"skipped, already done" //
												+ ", guid=" + lnkId //
												+ ", luid=" + lnkLuid //
												+ ", indexedAt=" + indexed //
												+ ", requestedAt=" + Format.Ecma.date(date)//
								);
								return;
							}
						}
					}
				}
			}
		}
		final LinkData link = MatLink.materializeActual(conn, lnkId);
		if (link != null) {
			final int luid = link.lnkLuid;
			Report.event(RunnerChangeUpdate.OWNER, "INDEXING", "update, guid=" + lnkId + ", luid=" + luid);
			this.doIndex(conn, link, luid);
			this.eventIndexed(conn, luid, false);
			return;
		}
		if (lnkLuid != -1 && !"$$ROOT_ENTRY".equals(lnkId)) {
			Report.event(RunnerChangeUpdate.OWNER, "CLEANING", "luid=" + lnkLuid);
			this.indexing.doDelete(conn, lnkLuid);
		}
	}

	private final void doUpdateAll(final Connection conn, final Map<String, Object> task) throws Exception {

		final String guid = Convert.MapEntry.toString(task, "evtCmdGuid", "").trim();
		this.invalidations.add(InvalidationEventType.UIDEN, guid);
		final List<Object> linkData = new ArrayList<>();
		try (final PreparedStatement ps = conn
				.prepareStatement("SELECT t.lnkId,t.lnkLuid,t.cntLnkId FROM s3Tree t WHERE t.objId=?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
			ps.setString(1, guid);
			try (final ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					final String lnkId = rs.getString(1);
					linkData.add(lnkId);
					linkData.add(Integer.valueOf(rs.getInt(2)));
					this.invalidations.add(InvalidationEventType.ULINK, lnkId);
					this.invalidations.add(InvalidationEventType.UTREE, rs.getString(3));
				}
			}
		}
		if (linkData.size() > 16) {
			for (int i = linkData.size(); i > 0;) {
				final int luid = ((Integer) linkData.get(--i)).intValue();
				final String id = (String) linkData.get(--i);
				MatChange.serialize(this.server, conn, 0, "update", id, luid);
			}
		} else {
			final long date = Convert.MapEntry.toLong(task, "evtDate", 0L);
			for (int i = linkData.size(); i > 0;) {
				final int luid = ((Integer) linkData.get(--i)).intValue();
				final String id = (String) linkData.get(--i);
				this.doUpdate(conn, id, luid, date);
			}
		}
	}

	/** @param conn
	 * @param id
	 */
	private void doUpdateExtra(final Connection conn, final String id) throws Exception {

		MatExtra.update(this.server, conn, id);
	}

	private final void doUpdateObject(final Connection conn, final Map<String, Object> task) throws Exception {

		final String guid = Convert.MapEntry.toString(task, "evtCmdGuid", "").trim();
		this.doUpdateObject(conn, guid);
	}

	private final void doUpdateObject(final Connection conn, final String guid) throws Exception {

		int count = 0;
		if (conn.getMetaData().supportsUnionAll()) {
			try (final PreparedStatement ps = conn.prepareStatement(
					"SELECT COUNT(*) FROM s3Tree t WHERE t.objId=? UNION ALL SELECT COUNT(*) FROM s3RecycledTree r WHERE r.objId=?",
					ResultSet.TYPE_FORWARD_ONLY,
					ResultSet.CONCUR_READ_ONLY)) {
				ps.setString(1, guid);
				ps.setString(2, guid);
				try (final ResultSet rs = ps.executeQuery()) {
					while (rs.next()) {
						count += rs.getInt(1);
					}
				}
			}
		} else {
			try (final PreparedStatement ps = conn.prepareStatement("SELECT COUNT(*) FROM s3Tree t WHERE t.objId=?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
				ps.setString(1, guid);
				try (final ResultSet rs = ps.executeQuery()) {
					if (rs.next()) {
						count += rs.getInt(1);
					}
				}
			}
			try (final PreparedStatement ps = conn
					.prepareStatement("SELECT COUNT(*) FROM s3RecycledTree r WHERE r.objId=?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
				ps.setString(1, guid);
				try (final ResultSet rs = ps.executeQuery()) {
					if (rs.next()) {
						count += rs.getInt(1);
					}
				}
			}
		}
		if (count == 0) {
			MatExtra.unlink(conn, null, guid);
			MatData.delete(conn, guid);
			MatHistory.clear(conn, guid);
			MatChange.serializeInvalidation(this.storage, conn, InvalidationEventType.UIDEN, guid);
		}
	}

	private final boolean doUpgradeIndex(final Connection conn) throws Exception {

		final List<Map.Entry<String, Integer>> toUpgrade = new ArrayList<>();
		try (final PreparedStatement ps = conn.prepareStatement(
				"SELECT t.lnkId,t.lnkLuid FROM s3Tree t,s3Indexed i WHERE t.lnkLuid=i.luid AND i.idxVersion<" + this.indexingVersion + " ORDER BY i.idxVersion ASC, t.lnkLuid DESC",
				ResultSet.TYPE_FORWARD_ONLY,
				ResultSet.CONCUR_READ_ONLY)) {
			ps.setMaxRows(RunnerChangeUpdate.LIMIT_BULK_UPGRADE);
			try (final ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					toUpgrade.add(new EntrySimple<>(rs.getString(1), Integer.valueOf(rs.getInt(2))));
				}
			}
		}
		if (toUpgrade.isEmpty()) {
			return false;
		}
		if (toUpgrade.size() == RunnerChangeUpdate.LIMIT_BULK_UPGRADE) {
			MatChange.serialize(this.server, conn, RunnerChangeUpdate.SEQ_HIGH, "upgrade-index", "*", this.indexingVersion);
		}
		for (final Map.Entry<String, Integer> entry : toUpgrade) {
			MatChange.serialize(this.server, conn, RunnerChangeUpdate.SEQ_MEDIUM, "update", entry.getKey(), entry.getValue().intValue());
		}
		return true;
	}

	private final void eventIndexed(final Connection conn, final int luid, final boolean created) throws SQLException {

		if (created && !this.failedLastEventIndexedCreate) {
			try (final PreparedStatement ps = conn.prepareStatement("INSERT INTO s3Indexed(luid,idxVersion,lnkIndexed) VALUES (" + luid + "," + this.indexingVersion + ",?)")) {
				ps.setTimestamp(1, new Timestamp(Engine.fastTime()));
				ps.execute();
			} catch (final SQLException e) {
				try (final PreparedStatement ps = conn.prepareStatement("UPDATE s3Indexed SET idxVersion=" + this.indexingVersion + ", lnkIndexed=? WHERE luid=" + luid)) {
					ps.setTimestamp(1, new Timestamp(Engine.fastTime()));
					ps.execute();
				} catch (final SQLException fatal) {
					this.failedLastEventIndexedCreate = true;
					throw new SQLException(
							"Got two errors in a row this trying opportunistic existance prediction: eventIndexed(" + luid + ", " + created
									+ ")  will try another method next time, unexpected second exception: " + fatal.getMessage(),
							e);
				}
			}
		} else {
			final int updated;
			try (final PreparedStatement ps = conn.prepareStatement("UPDATE s3Indexed SET idxVersion=" + this.indexingVersion + ", lnkIndexed=? WHERE luid=" + luid)) {
				ps.setTimestamp(1, new Timestamp(Engine.fastTime()));
				updated = ps.executeUpdate();
			}
			if (updated == 0) {
				try (final PreparedStatement ps = conn.prepareStatement("INSERT INTO s3Indexed(luid,idxVersion,lnkIndexed) VALUES (" + luid + "," + this.indexingVersion + ",?)")) {
					ps.setTimestamp(1, new Timestamp(Engine.fastTime()));
					ps.execute();
				}
			}
		}
	}

	private void fillTree(final Connection conn, final List<Object> list, final int luid) throws Exception {

		final List<Object> children = MatTree.children(conn, luid);
		for (final Iterator<Object> i = children.iterator(); i.hasNext();) {
			final Integer integer = (Integer) i.next();
			final String object = (String) i.next();
			list.add(integer);
			list.add(object);
			this.fillTree(conn, list, integer.intValue());
		}
	}

	private void fillTree(final Connection conn, final List<Object> list, final String lnkId) throws Exception {

		final List<Object> children = MatTree.children(conn, lnkId);
		for (final Iterator<Object> i = children.iterator(); i.hasNext();) {
			final Integer integer = (Integer) i.next();
			final String object = (String) i.next();
			list.add(integer);
			list.add(object);
			this.fillTree(conn, list, integer.intValue());
		}
	}

	@Override
	public int getVersion() {

		return 36;
	}

	private final void makeLost(final Connection conn, final LinkData entry, final boolean searchUpper) throws SQLException {

		final boolean drop;
		try (final PreparedStatement ps = conn
				.prepareStatement("SELECT count(*) FROM s3Tree t,s3Objects o WHERE t.objId=o.objId AND t.objId=?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
			/** Do not use it here, may be unsupported? */
			try {
				ps.setQueryTimeout(60);
			} catch (final Throwable t) {
				// ignore
			}
			ps.setString(1, entry.objId);
			try (final ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					drop = rs.getInt(1) > 1;
				} else {
					drop = true;
				}
			}
		}
		if (drop) {
			final BaseEntry<?> first = this.server.getStorage().getStorage().getByGuid(entry.lnkId);
			if (first != null) {
				final BaseChange change = first.createChange();
				change.unlink();
				Report.info(RunnerChangeUpdate.OWNER, "Lost item dropped, not last link: guid=" + entry.lnkId);
			}
			return;
		}
		final BaseEntry<?> root = this.server.getStorage().getInterface().getRoot();
		BaseEntry<?> lostAndFound = root.getChildByName("lost_found");
		if (lostAndFound == null) {
			final BaseChange change = root.createChild();
			change.setTypeName(this.storage.getServer().getTypes().getTypeNameDefault());
			change.setFolder(true);
			change.setKey("lost_found");
			change.setState(ModuleInterface.STATE_DRAFT);
			change.setTitle("! LOST + FOUND");
			change.setCreateLocal(true);
			change.commit();
			lostAndFound = root.getChildByName("lost_found");
		}
		final BaseEntry<?> first = this.server.getStorage().getStorage().getByGuid(entry.lnkId);
		BaseEntry<?> lost = first;
		if (searchUpper) {
			if (lost != null) {
				for (; lost.getParent() != null;) {
					lost = lost.getParent();
				}
				if ("$$ROOT_ENTRY".equals(lost.getGuid())) {
					lost = first;
				}
			}
		}
		if (lost != null) {
			final BaseEntry<?> existingLost = lostAndFound.getChildByName(lost.getKey());
			if (existingLost != null) {
				existingLost.createChange().delete();
			}
			final BaseChange change = lost.createChange();
			change.setParentGuid(lostAndFound.getGuid());
			change.setState(ModuleInterface.STATE_DRAFT);
			change.commit();
		}
	}

	@SuppressWarnings("resource")
	@Override
	public void run() {

		if (this.destroyed) {
			return;
		}
		final Connection conn;
		try {
			conn = this.storage.nextConnection();
			if (conn == null) {
				if (!this.destroyed) {
					Act.later(null, this, 10000L);
				}
				return;
			}
		} catch (final Throwable t) {
			if (!this.destroyed) {
				Act.later(null, this, 10000L);
			}
			return;
		}
		boolean highLoad = false;
		try {
			// do maintenance
			{
				final BaseObject settings = this.storage.getSettingsProtected();
				this.doMaintainVersionUpdate(conn, settings);
				this.doMaintainClearDead(conn, settings);
				this.doMaintainIndexing(conn, settings);
				this.storage.commitProtectedSettings();
			}
			// do update
			{
				final List<Map<String, Object>> tasks;
				try {
					try (final PreparedStatement ps = conn.prepareStatement(
							"SELECT evtId,evtCmdType,evtCmdGuid,evtCmdLuid FROM s3ChangeQueue ORDER BY evtSequence ASC, evtDate ASC",
							ResultSet.TYPE_FORWARD_ONLY,
							ResultSet.CONCUR_READ_ONLY)) {
						ps.setMaxRows(RunnerChangeUpdate.LIMIT_BULK_TASKS);
						try (final ResultSet rs = ps.executeQuery()) {
							if (rs.next()) {
								tasks = new ArrayList<>();
								do {
									final Map<String, Object> task = new TreeMap<>();
									task.put("evtId", rs.getString(1));
									task.put("evtCmdType", rs.getString(2));
									task.put("evtCmdGuid", rs.getString(3));
									task.put("evtCmdLuid", Integer.valueOf(rs.getInt(4)));
									tasks.add(task);
								} while (rs.next());
							} else {
								tasks = null;
							}
						}
					}
				} catch (final SQLException e) {
					throw new RuntimeException(e);
				}
				if (tasks == null) {
					final Iterator<String> iterator = this.server.updateExtra.iterator();
					for (int i = 5; i > 0 && iterator.hasNext(); --i) {
						final String id = iterator.next();
						try {
							this.doUpdateExtra(conn, id);
						} catch (final Throwable t) {
							Report.exception(RunnerChangeUpdate.OWNER, "Error while updating extra, id=" + id, t);
						}
						iterator.remove();
					}
					highLoad = iterator.hasNext();
				} else {
					highLoad = tasks.size() >= RunnerChangeUpdate.LIMIT_BULK_TASKS;
					try {
						conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
						conn.setAutoCommit(false);
						boolean gotIndexUpgrade = false;
						for (final Map<String, Object> task : tasks) {
							final String taskType = Convert.MapEntry.toString(task, "evtCmdType", "").trim();
							if ("create".equals(taskType)) {
								highLoad |= this.doCreateLocal(conn, task);
							} else //
							if ("create-global".equals(taskType)) {
								highLoad |= this.doCreateGlobal(conn, task);
							} else //
							if ("resync".equals(taskType)) {
								highLoad |= this.doResync(conn, task);
							} else //
							if ("update".equals(taskType)) {
								this.doUpdate(conn, task);
							} else //
							if ("update-all".equals(taskType)) {
								this.doUpdateAll(conn, task);
							} else //
							if ("update-object".equals(taskType)) {
								this.doUpdateObject(conn, task);
							} else //
							if ("clean".equals(taskType)) {
								this.doClean(conn, task);
							} else //
							if ("clean-start".equals(taskType)) {
								highLoad |= this.doCleanStart(conn, task);
							} else //
							if ("clean-all".equals(taskType)) {
								this.doCleanAll(conn, task);
							} else //
							if ("upgrade-index".equals(taskType)) {
								if (!gotIndexUpgrade) {
									final int toVersion = Convert.MapEntry.toInt(task, "evtCmdLuid", -1);
									if (this.indexingVersion < toVersion) {
										MatChange.serialize(this.server, conn, RunnerChangeUpdate.SEQ_HIGH, "upgrade-index", "*", toVersion);
										continue;
									}
									highLoad |= this.doUpgradeIndex(conn);
									gotIndexUpgrade = true;
								}
							} else //
							if ("delete-item".equals(taskType)) {
								this.doDeleteItem(conn, task);
							} else //
							if ("recycle-start".equals(taskType)) {
								highLoad |= this.doRecycleStart(conn, task);
							} else //
							if ("recycle-all".equals(taskType)) {
								this.doRecycleAll(conn, task);
							} else //
							if ("recycle-item".equals(taskType)) {
								this.doRecycleItem(conn, task);
							} else //
							if ("recycle-finish".equals(taskType)) {
								this.doRecycleFinish(conn, task);
							}
							RunnerChangeUpdate.eventDone(conn, Convert.MapEntry.toString(task, "evtId", "").trim());
							conn.commit();
							if (this.invalidations != null) {
								MatChange.serializeInvalidations(this.storage, conn, this.invalidations);
								conn.commit();
								this.invalidations.clear();
							}
							// restore fail counters
							/** cannot recover actually, connection dies in postgres..., so instead
							 * of '= false' will do probability */
							if (this.failedLastEventIndexedCreate) {
								this.failedLastEventIndexedCreate = Engine.createRandom(3) == 0;
							}
						}
					} catch (final Throwable e) {
						try {
							conn.rollback();
						} catch (final Throwable t) {
							// ignore
						}
						Report.exception(RunnerChangeUpdate.OWNER, "Exception while updating a storage", e);
					}
				}
			}
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
						highLoad
							? 2500L
							: 15000L);
			}
		}
	}

	@Override
	public void start() {

		this.serverPresence.setUpdateInterest(true);
		this.destroyed = false;
		Act.later(null, this, 2500L);
	}

	@Override
	public void stop() {

		this.serverPresence.setUpdateInterest(false);
		this.destroyed = true;
	}

	@Override
	public String toString() {

		return this.getClass().getSimpleName() + "(" + this.storage + ")";
	}
}
