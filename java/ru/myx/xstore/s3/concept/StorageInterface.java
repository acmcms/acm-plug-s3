/*
 * Created on 09.07.2004
 */
package ru.myx.xstore.s3.concept;

import java.util.Map;

import ru.myx.ae2.indexing.IndexingDictionary;
import ru.myx.ae2.indexing.IndexingStemmer;
import ru.myx.ae3.extra.ExternalHandler;
import ru.myx.ae3.status.StatusFiller;
import ru.myx.ae3.status.StatusInfo;

/**
 * @author myx
 * 
 */
public interface StorageInterface extends StatusFiller {
	/**
	 * @return handler
	 */
	ExternalHandler getExternalizerObject();
	
	/**
	 * @return handler
	 */
	ExternalHandler getExternalizerReference();
	
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
	 */
	LinkData getLink(final String guid);
	
	/**
	 * @param guid
	 * @return tree
	 */
	TreeData getTree(final String guid);
	
	/**
	 * 
	 */
	void invalidateCache();
	
	/**
	 * 
	 * @param identifier
	 */
	void invalidateDeleteExtra(final String identifier);
	
	/**
	 * 
	 * @param identifier
	 */
	void invalidateDeleteLink(final String identifier);
	
	/**
	 * 
	 * @param identifier
	 */
	void invalidateUpdateIdentity(final String identifier);
	
	/**
	 * 
	 * @param identifier
	 */
	void invalidateUpdateLink(final String identifier);
	
	/**
	 * 
	 * @param identifier
	 */
	void invalidateUpdateTree(final String identifier);
	
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
	Map.Entry<String, Object>[] search(
			final String lnkId,
			final int limit,
			final boolean all,
			final long timeout,
			final String sort,
			final long dateStart,
			final long dateEnd,
			final String filter);
	
	/**
	 * @param lnkId
	 * @param all
	 * @param timeout
	 * @param dateStart
	 * @param dateEnd
	 * @param filter
	 * @return
	 */
	Map<Integer, Map<Integer, Map<String, Number>>> searchCalendar(
			final String lnkId,
			final boolean all,
			final long timeout,
			final long dateStart,
			final long dateEnd,
			final String filter);
	
	/**
	 * @param guid
	 * @param all
	 * @return string array
	 * @throws Exception
	 */
	LinkData[] searchIdentity(final String guid, final boolean all) throws Exception;
	
	/**
	 * @param guid
	 * @param all
	 * @return string array
	 * @throws Exception
	 */
	LinkData[] searchLinks(final String guid, final boolean all) throws Exception;
	
	/**
	 * @param lnkId
	 * @param condition
	 * @return array ordered by object title
	 */
	LinkData[] searchLocal(final String lnkId, final String condition);
	
	/**
	 * 
	 */
	void start();
	
	@Override
	public void statusFill(StatusInfo data);
	
	/**
	 * 
	 */
	public void stop();
}
