/*
 * Created on 09.07.2004
 */
package ru.myx.xstore.s3.concept;

import java.util.Map;
import java.util.Set;

import ru.myx.ae1.storage.BaseHistory;
import ru.myx.ae1.storage.BaseVersion;
import ru.myx.ae1.storage.ModuleRecycler;
import ru.myx.ae1.storage.ModuleSchedule;
import ru.myx.ae1.storage.ModuleSynchronizer;
import ru.myx.ae2.indexing.IndexingDictionary;
import ru.myx.ae2.indexing.IndexingFinder;
import ru.myx.ae2.indexing.IndexingStemmer;
import ru.myx.ae3.binary.TransferCopier;
import ru.myx.ae3.status.StatusFiller;
import ru.myx.ae3.status.StatusInfo;
import ru.myx.jdbc.lock.LockManager;
import ru.myx.jdbc.queueing.RequestAttachment;
import ru.myx.jdbc.queueing.RunnerDatabaseRequestor;

/**
 * @author myx
 * 
 *         To change the template for this generated type comment go to
 *         Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
 */
public interface StorageNetwork extends ModuleRecycler, LockManager, StatusFiller {
	/**
	 * @return boolean
	 */
	boolean areSynchronizationsSupported();
	
	/**
	 * @return transaction
	 */
	Transaction createTransaction();
	
	/**
	 * @param task
	 */
	void enqueueTask(RequestAttachment<?, RunnerDatabaseRequestor> task);
	
	/**
	 * @param lnkId
	 * @return result
	 */
	String[] getAliases(final String lnkId);
	
	/**
	 * @return finder
	 */
	IndexingFinder getFinder();
	
	/**
	 * 
	 * @return identity
	 */
	String getIdentity();
	
	/**
	 * @return dictionary
	 */
	IndexingDictionary getIndexingDictionary();
	
	/**
	 * @return stemmer
	 */
	IndexingStemmer getIndexingStemmer();
	
	/**
	 * @return issuer
	 */
	Object getIssuer();
	
	/**
	 * @param guid
	 *            - link guid
	 * @return link
	 * @throws Exception
	 */
	LinkData getLink(final String guid) throws Exception;
	
	/**
	 * @param objId
	 * @return result
	 */
	BaseHistory[] getObjectHistory(final String objId);
	
	/**
	 * @param lnkId
	 * @param historyId
	 * @return result
	 */
	LinkData getObjectHistorySnapshot(final String lnkId, final String historyId);
	
	/**
	 * @param lnkId
	 * @param objId
	 * @param versionId
	 * @return result
	 */
	LinkData getObjectVersion(final String lnkId, final String objId, final String versionId);
	
	/**
	 * @param objId
	 * @return result
	 */
	BaseVersion[] getObjectVersions(final String objId);
	
	/**
	 * @return scheduler
	 */
	ModuleSchedule getScheduling();
	
	/**
	 * @return synchronizer
	 */
	ModuleSynchronizer getSynchronizer();
	
	/**
	 * @param guid
	 *            - link guid
	 * @return tree
	 * @throws Exception
	 */
	TreeData getTree(final String guid) throws Exception;
	
	/**
	 * @param attachment
	 * @param identifier
	 * @return boolean
	 * @throws Exception
	 */
	boolean hasExternal(final Object attachment, final String identifier) throws Exception;
	
	/**
	 * @param guids
	 * @return existing
	 */
	Set<String> hasExternals(final Set<String> guids);
	
	/**
	 * @param attachment
	 * @param identifier
	 * @param target
	 * @return boolean
	 * @throws Exception
	 */
	boolean loadExternal(final Object attachment, final String identifier, final TargetExternal target)
			throws Exception;
	
	/**
	 * @param attachment
	 * @param key
	 * @param type
	 * @param copier
	 * @return identifier
	 * @throws Exception
	 */
	public String putExternal(final Object attachment, final String key, final String type, final TransferCopier copier)
			throws Exception;
	
	/**
	 * @param lnkId
	 * @param limit
	 * @param all
	 * @param timeout
	 * @param sort
	 * @param dateStart
	 * @param dateEnd
	 * @param filter
	 * @return result
	 */
	public Map.Entry<String, Object>[] search(
			final String lnkId,
			final int limit,
			final boolean all,
			final long timeout,
			final String sort,
			final long dateStart,
			final long dateEnd,
			final String filter);
	
	/**
	 * @param guid
	 * @return string array
	 * @throws Exception
	 */
	public LinkData[] searchIdentity(final String guid) throws Exception;
	
	/**
	 * @param guid
	 * @return string array
	 * @throws Exception
	 */
	public LinkData[] searchLinks(final String guid) throws Exception;
	
	/**
	 * @param lnkId
	 * @param condition
	 * @return array ordered by object title
	 */
	public LinkData[] searchLocal(final String lnkId, final String condition);
	
	/**
	 * 
	 */
	public void start();
	
	/**
	 * 
	 */
	public void statIncrementSearch();
	
	/**
	 * 
	 */
	public void statIncrementSearchCalendar();
	
	/**
	 * 
	 */
	public void statIncrementSearchCalendarEmpty();
	
	/**
	 * 
	 */
	public void statIncrementSearchCalendarItem();
	
	/**
	 * 
	 */
	public void statIncrementSearchEmpty();
	
	/**
	 * 
	 */
	public void statIncrementSearchItem();
	
	/**
	 * 
	 */
	public void statIncrementSearchTimeout();
	
	@Override
	public void statusFill(final StatusInfo data);
	
	/**
	 * 
	 */
	public void stop();
}
