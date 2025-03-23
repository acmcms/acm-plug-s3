/*
 * Created on 09.07.2004
 */
package ru.myx.xstore.s3.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import ru.myx.ae1.schedule.Scheduling;
import ru.myx.ae1.storage.BaseHistory;
import ru.myx.ae1.storage.BaseRecycled;
import ru.myx.ae1.storage.BaseVersion;
import ru.myx.ae1.storage.ModuleSchedule;
import ru.myx.ae1.storage.ModuleSynchronizer;
import ru.myx.ae2.indexing.Indexing;
import ru.myx.ae2.indexing.IndexingDictionary;
import ru.myx.ae2.indexing.IndexingFinder;
import ru.myx.ae2.indexing.IndexingStemmer;
import ru.myx.ae3.Engine;
import ru.myx.ae3.act.Act;
import ru.myx.ae3.base.BaseObject;
import ru.myx.ae3.binary.TransferCopier;
import ru.myx.ae3.exec.Exec;
import ru.myx.ae3.extra.ExternalHandler;
import ru.myx.ae3.help.Create;
import ru.myx.ae3.help.Format;
import ru.myx.ae3.status.StatusFiller;
import ru.myx.ae3.status.StatusInfo;
import ru.myx.jdbc.lock.Interest;
import ru.myx.jdbc.lock.Lock;
import ru.myx.jdbc.lock.LockManager;
import ru.myx.jdbc.lock.Locker;
import ru.myx.jdbc.queueing.RequestAttachment;
import ru.myx.jdbc.queueing.RunnerDatabaseRequestor;
import ru.myx.util.Counter;
import ru.myx.xstore.s3.StorageLevel3;
import ru.myx.xstore.s3.concept.LinkData;
import ru.myx.xstore.s3.concept.RequestSearch;
import ru.myx.xstore.s3.concept.StorageNetwork;
import ru.myx.xstore.s3.concept.TargetExternal;
import ru.myx.xstore.s3.concept.Transaction;
import ru.myx.xstore.s3.concept.TreeData;

/** @author myx */
public final class ServerJdbc implements StorageNetwork {
	
	private static final Map<String, String> FINDER_REPLACEMENT_FIELDS;
	static {
		final Map<String, String> result = new HashMap<>();
		result.put("$created", "o.objCreated");
		result.put("$modified", "o.objDate");
		result.put("$title", "o.objTitle");
		result.put("$key", "t.lnkName");
		result.put("$type", "o.objType");
		FINDER_REPLACEMENT_FIELDS = result;
	}
	
	private final boolean client;
	
	private IndexingFinder currentFinder;
	
	private Indexing currentIndexing;
	
	private ModuleSchedule currentScheduling;
	
	private ModuleSynchronizer currentSynchronizer;
	
	private final String identity;
	
	private final DictionaryJdbc indexingDictionary;
	
	private final Object issuer = new Object();
	
	private RunnerLinkLoader linkLoader;
	
	private LockManager lockManager;
	
	private final boolean scheduling;
	
	private RunnerDatabaseRequestor searchLoader;
	
	private RunnerServerPresence serverPresence;
	
	private final StorageLevel3 storage;
	
	int stsExternalsUpdated = 0;
	
	private int stsGetAliasesAttempted;
	
	private int stsGetExternalAttempted;
	
	private int stsGetLinkAttempted;
	
	private int stsGetTreeAttempted;
	
	private int stsHasExternalAttempted;
	
	private int stsHasExternalItemsAttempted;
	
	private int stsHasExternalsAttempted;
	
	int stsLoadExternal = 0;
	
	int stsLoadExternalFailed = 0;
	
	int stsLoadExternalPlain = 0;
	
	int stsLoadExternalTry = 0;
	
	int stsLoadExternalUngzip = 0;
	
	int stsLoadExternalUnknown = 0;
	
	private int stsPutExternalAttempted;
	
	private int stsSearchAttempted;
	
	private int stsSearchEmptyAttempted;
	
	private int stsSearchItemAttempted;
	
	private int stsSearchTimedOut;
	
	private int stsSearchCalendarAttempted;
	
	private int stsSearchCalendarEmptyAttempted;
	
	private int stsSearchCalendarItemAttempted;
	
	private int stsSearchIdentityAttempted;
	
	private int stsSearchLinksAttempted;
	
	private int stsSearchLocalAttempted;
	
	private int stsTransactionsAttempted;
	
	int stsTransactionsCommited;
	
	private int stsTransactionsCreated;
	
	int stsTransactionsRollbacks;
	
	private RunnerTreeLoader treeLoader;
	
	final Set<String> updateExtra = Create.tempSet();
	
	Counter stsTransactionTime = new Counter();
	
	private int stsGetObjectHistory = 0;
	
	private int stsGetObjectHistorySnapshot = 0;
	
	private int stsGetObjectVersionSnapshot = 0;
	
	private int stsGetObjectVersions = 0;
	
	private int stsGetRecycledList;
	
	private int stsGetRecycledObject;
	
	/** @param storage
	 * @param identity
	 * @param scheduling
	 * @param client */
	public ServerJdbc(final StorageLevel3 storage, final String identity, final boolean scheduling, final boolean client) {
		
		this.storage = storage;
		this.identity = identity;
		this.client = client;
		this.scheduling = scheduling;
		this.indexingDictionary = new DictionaryJdbc(storage);
	}
	
	@Override
	public final boolean addInterest(final Interest interest) {
		
		return this.lockManager.addInterest(interest);
	}
	
	@Override
	public final boolean areSynchronizationsSupported() {
		
		return true;
	}
	
	@Override
	public final void clearAllRecycled() {
		
		try (final Connection conn = this.storage.nextConnection()) {
			if (conn == null) {
				throw new RuntimeException("Database is not available!");
			}
			try {
				conn.setAutoCommit(false);
				MatRecycled.clearRecycled(conn);
				conn.commit();
			} catch (final Throwable t) {
				try {
					conn.rollback();
				} catch (final Throwable tt) {
					// ignore
				}
				throw t;
			}
		} catch (final Error | RuntimeException e) {
			throw e;
		} catch (final Throwable t) {
			throw new RuntimeException(t);
		}
	}
	
	@Override
	public final Locker createLock(final String guid, final int version) {
		
		return this.lockManager.createLock(guid, version);
	}
	
	@Override
	public final Transaction createTransaction() {
		
		this.stsTransactionsAttempted++;
		final Connection conn = this.storage.nextConnection();
		if (conn == null) {
			throw new RuntimeException("Database is not available!");
		}
		try {
			conn.setAutoCommit(false);
			this.stsTransactionsCreated++;
			return new TransactionJdbc(this, conn, this.issuer);
		} catch (final SQLException e) {
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public void enqueueTask(final RequestAttachment<?, RunnerDatabaseRequestor> task) {
		
		this.searchLoader.add(task);
	}
	
	@Override
	public final String[] getAliases(final String lnkId) {
		
		this.stsGetAliasesAttempted++;
		final RequestAliases request = new RequestAliases(lnkId);
		this.searchLoader.add(request);
		return request.baseValue();
	}
	
	@Override
	public final IndexingFinder getFinder() {
		
		return this.currentFinder;
	}
	
	@Override
	public final String getIdentity() {
		
		return this.identity;
	}
	
	@Override
	public final IndexingDictionary getIndexingDictionary() {
		
		return this.indexingDictionary;
	}
	
	@Override
	public final IndexingStemmer getIndexingStemmer() {
		
		return IndexingStemmer.SIMPLE_STEMMER;
	}
	
	@Override
	public final Object getIssuer() {
		
		return this.issuer;
	}
	
	@Override
	public final LinkData getLink(final String guid) throws Exception {
		
		this.stsGetLinkAttempted++;
		final LinkData result = new LinkData(guid);
		this.linkLoader.add(result);
		return result.getLoadValue();
	}
	
	@Override
	public final BaseHistory[] getObjectHistory(final String objId) {
		
		this.stsGetObjectHistory++;
		try (final Connection conn = this.storage.nextConnection()) {
			if (conn == null) {
				throw new RuntimeException("Database is not available!");
			}
			return MatHistory.materialize(conn, objId);
		} catch (final RuntimeException e) {
			throw e;
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public final LinkData getObjectHistorySnapshot(final String lnkId, final String historyId) {
		
		this.stsGetObjectHistorySnapshot++;
		try (final Connection conn = this.storage.nextConnection()) {
			if (conn == null) {
				throw new RuntimeException("Database is not available!");
			}
			final LinkData link = MatLink.materializeHistory(conn, lnkId, historyId);
			if (link == null) {
				return null;
			}
			return link;
		} catch (final RuntimeException e) {
			throw e;
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public final LinkData getObjectVersion(final String lnkId, final String objId, final String versionId) {
		
		this.stsGetObjectVersionSnapshot++;
		try (final Connection conn = this.storage.nextConnection()) {
			if (conn == null) {
				throw new RuntimeException("Database is not available!");
			}
			final BaseObject data = MatVersion.materializeSnapshot(this, conn, objId, versionId);
			if (data == null || data == BaseObject.UNDEFINED) {
				return null;
			}
			final LinkData link = MatLink.materializeVersion(conn, lnkId, versionId);
			if (link == null) {
				return null;
			}
			link.setDataReal(data);
			return link;
		} catch (final RuntimeException e) {
			throw e;
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public final BaseVersion[] getObjectVersions(final String objId) {
		
		this.stsGetObjectVersions++;
		try (final Connection conn = this.storage.nextConnection()) {
			if (conn == null) {
				throw new RuntimeException("Database is not available!");
			}
			return MatVersion.materialize(conn, objId);
		} catch (final RuntimeException e) {
			throw e;
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public final BaseRecycled[] getRecycled() {
		
		this.stsGetRecycledList++;
		try (final Connection conn = this.storage.nextConnection()) {
			if (conn == null) {
				throw new RuntimeException("Database is not available!");
			}
			return MatRecycled.materializeAll(this, conn);
		} catch (final RuntimeException e) {
			throw e;
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public final BaseRecycled getRecycledByGuid(final String guid) {
		
		this.stsGetRecycledObject++;
		try (final Connection conn = this.storage.nextConnection()) {
			if (conn == null) {
				throw new RuntimeException("Database is not available!");
			}
			return MatRecycled.materialize(this, conn, guid);
		} catch (final RuntimeException e) {
			throw e;
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public final ModuleSchedule getScheduling() {
		
		return this.currentScheduling;
	}
	
	@Override
	public final ModuleSynchronizer getSynchronizer() {
		
		return this.currentSynchronizer;
	}
	
	@Override
	public final TreeData getTree(final String guid) throws Exception {
		
		this.stsGetTreeAttempted++;
		final TreeData result = new TreeData(guid);
		this.treeLoader.add(result);
		return result.baseValue();
	}
	
	@Override
	public final boolean hasExternal(final Object attachment, final String identifier) throws Exception {
		
		this.stsHasExternalAttempted++;
		this.stsHasExternalItemsAttempted++;
		if (attachment != null) {
			final StoreInfo info = (StoreInfo) attachment;
			final Connection conn = info.getConnection();
			return MatExtra.contains(conn, identifier);
		}
		final RequestHasExternal request = new RequestHasExternal(identifier);
		this.searchLoader.add(request);
		return Boolean.TRUE == request.baseValue();
	}
	
	@Override
	public Set<String> hasExternals(final Set<String> guids) {
		
		this.stsHasExternalsAttempted++;
		this.stsHasExternalItemsAttempted++;
		final RequestHasExternals request = new RequestHasExternals(guids);
		this.searchLoader.add(request);
		return request.baseValue();
	}
	
	@Override
	public final boolean loadExternal(final Object attachment, final String identifier, final TargetExternal target) throws Exception {
		
		this.stsGetExternalAttempted++;
		if (attachment != null) {
			if (attachment instanceof StoreInfo) {
				final StoreInfo info = (StoreInfo) attachment;
				final Connection conn = info.getConnection();
				return MatExtra.materialize(this, conn, identifier, target);
			}
			final Connection conn = (Connection) attachment;
			return MatExtra.materialize(this, conn, identifier, target);
		}
		final RequestLoadExternal request = new RequestLoadExternal(this, identifier, target);
		this.searchLoader.add(request);
		return Boolean.TRUE == request.baseValue();
	}
	
	@Override
	public final String putExternal(final Object attachment, final String key, final String type, final TransferCopier copier) throws Exception {
		
		this.stsPutExternalAttempted++;
		if (attachment != null) {
			final String identity = Engine.createGuid();
			final StoreInfo info = (StoreInfo) attachment;
			final Connection conn = info.getConnection();
			MatExtra.serialize(conn, identity, info.getObjId(), key, Engine.fastTime(), type, copier, info.preferInsert);
			return identity;
		}
		throw new RuntimeException("StoreInfo is not available!");
	}
	
	@Override
	public final boolean removeInterest(final Interest interest) {
		
		return this.lockManager.removeInterest(interest);
	}
	
	@Override
	public final Map.Entry<String, Object>[]
			search(final String lnkId, final int limit, final boolean all, final long timeout, final String sort, final long dateStart, final long dateEnd, final String filter) {
		
		final RequestSearch attachment = new RequestSearch(this.currentFinder, lnkId, limit, all, timeout, sort, dateStart, dateEnd, filter);
		return attachment.doSearch(this);
	}
	
	@Override
	public final LinkData[] searchIdentity(final String guid) throws Exception {
		
		this.stsSearchIdentityAttempted++;
		final RequestSearchIdentity request = new RequestSearchIdentity(guid);
		this.searchLoader.add(request);
		return request.baseValue();
	}
	
	@Override
	public final LinkData[] searchLinks(final String guid) throws Exception {
		
		this.stsSearchLinksAttempted++;
		final RequestSearchLinks request = new RequestSearchLinks(guid);
		this.searchLoader.add(request);
		return request.baseValue();
	}
	
	@Override
	public final LinkData[] searchLocal(final String lnkId, final String condition) {
		
		this.stsSearchLocalAttempted++;
		final String key = new StringBuilder().append(lnkId).append('\n').append(condition).toString();
		final RequestSearchLocal request = new RequestSearchLocal(key, lnkId, condition);
		this.searchLoader.add(request);
		return request.baseValue();
	}
	
	@Override
	public final void start() {
		
		this.currentSynchronizer = new SynchronizerJdbc(this);
		final StorageLevel3 storage = this.storage;
		this.currentIndexing = new Indexing(storage.getIndexingStemmer(), storage.getIndexingDictionary(), "s3Indices");
		this.currentFinder = new IndexingFinder(
				this.currentIndexing.getStemmer(),
				this.currentIndexing.getDictionary(),
				"s3Indices",
				ServerJdbc.FINDER_REPLACEMENT_FIELDS,
				"t.lnkId",
				"o.objCreated",
				"s3Tree t,s3Objects o WHERE ix.luid=t.lnkLuid AND t.objId=o.objId AND o.objState IN ('2','5') AND (",
				"s3Tree t,s3Objects o WHERE ix.luid=t.lnkLuid AND t.objId=o.objId AND (");
		this.currentScheduling = Scheduling.getScheduling(storage, "s3ScheduleQueue", "s3ScheduleLog");
		this.lockManager = Lock.createManager(storage.getConnectionSource(), "s3Locks", this.identity);
		this.serverPresence = new RunnerServerPresence(storage, this.client);
		this.serverPresence.start();
		if (this.lockManager != null) {
			if (!this.client) {
				this.lockManager.addInterest(new Interest("update", new RunnerChangeUpdate(storage, this, this.serverPresence, this.currentIndexing)));
				if (this.scheduling && this.currentScheduling != null) {
					this.lockManager.addInterest(new Interest("schedule", new RunnerScheduling(this.currentScheduling)));
				}
			}
			this.lockManager.start(this.identity);
		}
		
		this.linkLoader = new RunnerLinkLoader(this);
		Act.launchService(Exec.createProcess(null, "LINK: " + this.toString()), this.linkLoader);
		
		this.treeLoader = new RunnerTreeLoader(storage.getServer().getZoneId(), storage.getConnectionSource());
		Act.launchService(Exec.createProcess(null, "TREE: " + this.toString()), this.treeLoader);
		
		this.searchLoader = new RunnerDatabaseRequestor("SEARCHER", storage.getConnectionSource());
		Act.launchService(Exec.createProcess(null, "SEARCH: " + this.toString()), this.searchLoader);
		
	}
	
	@Override
	public final void start(final String identity) {
		
		this.lockManager.start(identity);
		/** FIXME: wtf? why service is started twice?! it is already started in start() */
		Act.launchService(Exec.createProcess(null, "LINK2: " + this.toString()), this.linkLoader);
	}
	
	@Override
	public void statIncrementSearch() {
		
		this.stsSearchAttempted++;
	}
	
	@Override
	public void statIncrementSearchCalendar() {
		
		this.stsSearchCalendarAttempted++;
	}
	
	@Override
	public void statIncrementSearchCalendarEmpty() {
		
		this.stsSearchCalendarEmptyAttempted++;
	}
	
	@Override
	public void statIncrementSearchCalendarItem() {
		
		this.stsSearchCalendarItemAttempted++;
	}
	
	@Override
	public void statIncrementSearchEmpty() {
		
		this.stsSearchEmptyAttempted++;
	}
	
	@Override
	public void statIncrementSearchItem() {
		
		this.stsSearchItemAttempted++;
	}
	
	@Override
	public void statIncrementSearchTimeout() {
		
		this.stsSearchTimedOut++;
	}
	
	@Override
	public final void statusFill(final StatusInfo data) {
		
		data.put("NET, identity", this.identity);
		data.put("NET, transactions attempted", Format.Compact.toDecimal(this.stsTransactionsAttempted));
		data.put("NET, transactions created", Format.Compact.toDecimal(this.stsTransactionsCreated));
		data.put("NET, transactions commited", Format.Compact.toDecimal(this.stsTransactionsCommited));
		data.put("NET, transaction rollbacks", Format.Compact.toDecimal(this.stsTransactionsRollbacks));
		data.put("NET, transactions finalized", Format.Compact.toDecimal(this.stsTransactionTime.getCount()));
		data.put("NET, transaction: average duration", Format.Compact.toPeriod(this.stsTransactionTime.getAverage()));
		data.put("NET, transaction: maximal duration", Format.Compact.toPeriod(this.stsTransactionTime.getMaximum()));
		data.put("NET, transaction: total time spent", Format.Compact.toPeriod(this.stsTransactionTime.doubleValue()));
		data.put("NET, getAlias attempted", Format.Compact.toDecimal(this.stsGetAliasesAttempted));
		data.put("NET, getLink attempted", Format.Compact.toDecimal(this.stsGetLinkAttempted));
		data.put("NET, getTree attempted", Format.Compact.toDecimal(this.stsGetTreeAttempted));
		data.put("NET, hasExtenal attempted", Format.Compact.toDecimal(this.stsHasExternalAttempted));
		data.put("NET, hasExternal items attempted", Format.Compact.toDecimal(this.stsHasExternalItemsAttempted));
		data.put("NET, hasExternals attempted", Format.Compact.toDecimal(this.stsHasExternalsAttempted));
		data.put("NET, getExternal attempted", Format.Compact.toDecimal(this.stsGetExternalAttempted));
		data.put("NET, putExternal attempted", Format.Compact.toDecimal(this.stsPutExternalAttempted));
		data.put("NET, getObjectHistory attempted", Format.Compact.toDecimal(this.stsGetObjectHistory));
		data.put("NET, getObjectHistorySnapshot attempted", Format.Compact.toDecimal(this.stsGetObjectHistorySnapshot));
		data.put("NET, getObjectVersions attempted", Format.Compact.toDecimal(this.stsGetObjectVersions));
		data.put("NET, getObjectVersionSnapshot attempted", Format.Compact.toDecimal(this.stsGetObjectVersionSnapshot));
		data.put("NET, getRecycledList attempted", Format.Compact.toDecimal(this.stsGetRecycledList));
		data.put("NET, getRecycledObject attempted", Format.Compact.toDecimal(this.stsGetRecycledObject));
		data.put("NET, search attempted", Format.Compact.toDecimal(this.stsSearchAttempted));
		data.put("NET, search empty queries", Format.Compact.toDecimal(this.stsSearchEmptyAttempted));
		data.put("NET, search items attempted", Format.Compact.toDecimal(this.stsSearchItemAttempted));
		data.put("NET, search timeouts", Format.Compact.toDecimal(this.stsSearchTimedOut));
		data.put("NET, searchCalendar attempted", Format.Compact.toDecimal(this.stsSearchCalendarAttempted));
		data.put("NET, searchCalendar empty queries", Format.Compact.toDecimal(this.stsSearchCalendarEmptyAttempted));
		data.put("NET, searchCalendar items attempted", Format.Compact.toDecimal(this.stsSearchCalendarItemAttempted));
		data.put("NET, searchIdentity attempted", Format.Compact.toDecimal(this.stsSearchIdentityAttempted));
		data.put("NET, searchLinks attempted", Format.Compact.toDecimal(this.stsSearchLinksAttempted));
		data.put("NET, searchLocal attempted", Format.Compact.toDecimal(this.stsSearchLocalAttempted));
		data.put("NET-EXTR, load external total", Format.Compact.toDecimal(this.stsLoadExternal));
		data.put("NET-EXTR, load external ungzip", Format.Compact.toDecimal(this.stsLoadExternalUngzip));
		data.put("NET-EXTR, load external plain", Format.Compact.toDecimal(this.stsLoadExternalPlain));
		data.put("NET-EXTR, load external try", Format.Compact.toDecimal(this.stsLoadExternalTry));
		data.put("NET-EXTR, load external unknown", Format.Compact.toDecimal(this.stsLoadExternalUnknown));
		data.put("NET-EXTR, load external failed", Format.Compact.toDecimal(this.stsLoadExternalFailed));
		data.put("NET-EXTR, externals updated", Format.Compact.toDecimal(this.stsExternalsUpdated));
		data.put("NET-EXTR, update external queue", Format.Compact.toDecimal(this.updateExtra.size()));
		
		{
			final StatusFiller provider = this.indexingDictionary;
			if (provider != null) {
				provider.statusFill(data);
			}
		}
		{
			final StatusFiller provider = this.serverPresence;
			if (provider != null) {
				provider.statusFill(data);
			}
		}
		{
			final StatusFiller provider = this.treeLoader;
			if (provider != null) {
				provider.statusFill(data);
			}
		}
		{
			final StatusFiller provider = this.searchLoader;
			if (provider != null) {
				provider.statusFill(data);
			}
		}
	}
	
	@Override
	public final void stop() {
		
		if (this.searchLoader != null) {
			this.searchLoader.stop();
			this.searchLoader = null;
		}
		if (this.treeLoader != null) {
			this.treeLoader.stop();
			this.treeLoader = null;
		}
		if (this.linkLoader != null) {
			this.linkLoader.stop();
			this.linkLoader = null;
		}
		if (this.serverPresence != null) {
			this.serverPresence.stop();
			this.serverPresence = null;
		}
		if (this.lockManager != null) {
			this.lockManager.stop(this.identity);
			this.lockManager = null;
		}
	}
	
	@Override
	public final void stop(final String identity) {
		
		this.lockManager.stop(identity);
		this.linkLoader.stop();
	}
	
	@Override
	public String toString() {
		
		return "ServerJdbc [identity=" + this.identity + ", storage=" + this.storage + "]";
	}
	
	final StorageLevel3 getStorage() {
		
		return this.storage;
	}
	
	ExternalHandler getStorageExternalizerObject() {
		
		return this.storage.getServerInterface().getExternalizerObject();
	}
	
	ExternalHandler getStorageExternalizerReference() {
		
		return this.storage.getServerInterface().getExternalizerReference();
	}
	
	void updateExtra(final String recId) {
		
		this.updateExtra.add(recId);
	}
	
}
