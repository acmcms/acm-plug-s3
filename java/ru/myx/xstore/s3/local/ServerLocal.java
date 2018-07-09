/*
 * Created on 09.07.2004
 */
package ru.myx.xstore.s3.local;

import java.io.File;
import java.io.InputStream;
import java.util.Collections;
import java.util.Map;

import ru.myx.ae2.indexing.IndexingDictionary;
import ru.myx.ae2.indexing.IndexingDictionaryCached;
import ru.myx.ae2.indexing.IndexingDictionaryVfs;
import ru.myx.ae2.indexing.IndexingStemmer;
import ru.myx.ae3.Engine;
import ru.myx.ae3.binary.TransferCopier;
import ru.myx.ae3.cache.CacheL2;
import ru.myx.ae3.cache.CacheL3;
import ru.myx.ae3.extra.External;
import ru.myx.ae3.extra.ExternalHandler;
import ru.myx.ae3.help.Convert;
import ru.myx.ae3.help.Format;
import ru.myx.ae3.status.StatusInfo;
import ru.myx.ae3.vfs.Entry;
import ru.myx.ae3.vfs.EntryContainer;
import ru.myx.io.DataInputBufferedReusable;
import ru.myx.io.DataOutputBufferedReusable;
import ru.myx.util.QueueStackRecord;
import ru.myx.xstore.s3.StorageLevel3;
import ru.myx.xstore.s3.concept.InvalidationEventType;
import ru.myx.xstore.s3.concept.LinkData;
import ru.myx.xstore.s3.concept.StorageInterface;
import ru.myx.xstore.s3.concept.StorageNetwork;
import ru.myx.xstore.s3.concept.TargetExternal;
import ru.myx.xstore.s3.concept.TreeData;

/** @author myx */
public class ServerLocal implements StorageInterface, ExternalHandler, TargetExternal {

	private static final int FOLDERS_COUNT = 1024;

	private static final int FOLDERS_MASK = ServerLocal.FOLDERS_COUNT - 1;

	private static final int LEAF_COUNT = 16;

	private static final int LEAF_MASK = ServerLocal.LEAF_COUNT - 1;

	private static final Map.Entry<String, Object>[] NULL_OBJECT_OBJS = Convert.Array.toAny(new Map.Entry[0]);

	private static final DataInputBufferedReusable[] DATA_INPUTS = ServerLocal.createStaticDataInputs();

	private static final DataOutputBufferedReusable[] DATA_OUTPUTS = ServerLocal.createStaticDataOutputs();

	private static final Map<Integer, Map<Integer, Map<String, Number>>> NULL_OBJECT_CALENDAR = Collections.emptyMap();

	private static final DataInputBufferedReusable[] createStaticDataInputs() {

		final DataInputBufferedReusable[] result = new DataInputBufferedReusable[ServerLocal.LEAF_COUNT];
		for (int i = ServerLocal.LEAF_MASK; i >= 0; --i) {
			result[i] = new DataInputBufferedReusable();
		}
		return result;
	}

	private static final DataOutputBufferedReusable[] createStaticDataOutputs() {

		final DataOutputBufferedReusable[] result = new DataOutputBufferedReusable[ServerLocal.LEAF_COUNT];
		for (int i = ServerLocal.LEAF_MASK; i >= 0; --i) {
			result[i] = new DataOutputBufferedReusable();
		}
		return result;
	}

	private final StorageLevel3 storage;

	private final File folderLocal;

	private final IndexingDictionaryCached indexingDictionary;

	private final ExternalizerReference externalizerReference;

	private final Leaf[] leafs;

	private final StorageNetwork network;

	private final CacheL2<Object> cacheTreeSearch;

	private final CacheL2<int[]> cacheDictionary;

	private final CreatorSearch createSearch;

	private final CreatorSearchCalendar createSearchCalendar;

	private final QueueStackRecord<DiscardRecord> discardQueue = new QueueStackRecord<>();

	private int stsChecks = 0;

	private int stsHasExternal = 0;

	private int stsPutExternal = 0;

	private int stsExternal = 0;

	private int stsExternalReference = 0;

	private int stsLink = 0;

	private int stsTree = 0;

	private int stsSearchIdentity = 0;

	private int stsSearchLinks = 0;

	private int stsSearchLocal = 0;

	private int stsSearch = 0;

	private int stsSearchCalendar = 0;

	private int stsULINK = 0;

	private int stsDLINK = 0;

	private int stsUTREE = 0;

	private int stsUIDEN = 0;

	private int stsUEXTR = 0;

	private int leafIndex = (int) (System.currentTimeMillis() / 1024);

	/** @param storage
	 * @param folder
	 * @param cache
	 * @param cachePrefix
	 */
	public ServerLocal(final StorageLevel3 storage, final File folder, final CacheL3<Object> cache, final String cachePrefix) {
		
		this.storage = storage;
		this.folderLocal = new File(folder, "extra");
		this.folderLocal.mkdirs();
		this.externalizerReference = new ExternalizerReference(this);
		/** FIXME: too much caches, change to guid, cid and CacheL2 Server.getCacheRendered()??? */
		this.cacheTreeSearch = cache.getCacheL2(cachePrefix + "-t");
		this.createSearch = new CreatorSearch(this.storage.getServerNetwork(), ServerLocal.NULL_OBJECT_OBJS);
		this.createSearchCalendar = new CreatorSearchCalendar(this.storage.getServerNetwork(), ServerLocal.NULL_OBJECT_CALENDAR);
		this.cacheDictionary = cache.getCacheL2(cachePrefix + "_dict");
		{
			final Entry rootEntry = storage.getServer().getVfsRootEntry();
			final EntryContainer storageRoot = rootEntry.relativeFolderEnsure("storage/cache/s3/" + storage.getIdentity());
			final EntryContainer dictExactRoot = storageRoot.relativeFolderEnsure("dict-exact");
			final EntryContainer dictInexactRoot = storageRoot.relativeFolderEnsure("dict-inxact");
			this.indexingDictionary = new IndexingDictionaryCached(
					new IndexingDictionaryVfs(dictExactRoot, dictInexactRoot, this.storage.getServerNetwork().getIndexingDictionary()),
					this.cacheDictionary);
		}
		{
			this.network = storage.getServerNetwork();
			final File[] folders = new File[ServerLocal.FOLDERS_COUNT];
			this.leafs = new Leaf[ServerLocal.LEAF_COUNT];
			for (int i = ServerLocal.LEAF_MASK; i >= 0; --i) {
				this.leafs[i] = new Leaf(ServerLocal.DATA_INPUTS[i], ServerLocal.DATA_OUTPUTS[i], storage, this, this.network, storage.getServerNetwork().getIssuer(), folders);
			}
		}
	}

	@Override
	public final int accept(final String identifier, final long date, final String type, final InputStream data) throws Exception {

		final int index = identifier.hashCode();
		return this.leafs[index & ServerLocal.LEAF_MASK].accept(index & ServerLocal.FOLDERS_MASK, identifier, date, type, data);
	}

	final void checkDiscard() {

		for (;;) {
			final DiscardRecord record;
			synchronized (this.discardQueue) {
				record = this.discardQueue.first();
				if (record == null) {
					return;
				}
				if (record.date > Engine.fastTime()) {
					return;
				}
				this.discardQueue.next();
			}
			record.invalidator.invalidateOn(this, record.guid);
		}
	}

	@Override
	public boolean checkIssuer(final Object issuer) {

		return issuer != null && issuer == this.storage.getServerNetwork().getIssuer();
	}

	final void checkOnce() {

		try {
			final int index = this.leafIndex++;
			this.stsChecks++;
			this.leafs[index & ServerLocal.LEAF_MASK].check(index & ServerLocal.FOLDERS_MASK);
		} catch (final Exception e) {
			throw new RuntimeException("checkOnce", e);
		}
	}

	final void enqueueDiscard(final String guid, final InvalidationEventType invalidator) {

		synchronized (this.discardQueue) {
			this.discardQueue.enqueue(new DiscardRecord(Engine.fastTime() + 1000L * 60L, guid, invalidator));
		}
	}

	@Override
	public final External getExternal(final Object attachment, final String identifier) throws Exception {

		this.stsExternal++;
		final int index = identifier.hashCode();
		return this.leafs[index & ServerLocal.LEAF_MASK].getExternal(index & ServerLocal.FOLDERS_MASK, attachment, identifier);
	}

	@Override
	public final ExternalHandler getExternalizerObject() {

		return this;
	}

	@Override
	public final ExternalHandler getExternalizerReference() {

		return this.externalizerReference;
	}

	final External getExternalReference(final String identifier) {

		this.stsExternalReference++;
		final int index = identifier.hashCode();
		return new ExtraLocal(identifier, this.leafs[index & ServerLocal.LEAF_MASK], index & ServerLocal.FOLDERS_MASK);
	}

	final File getFolderLocal() {

		return this.folderLocal;
	}

	@Override
	public final IndexingDictionary getIndexingDictionary() {

		return this.indexingDictionary;
	}

	@Override
	public final IndexingStemmer getIndexingStemmer() {

		return this.storage.getServerNetwork().getIndexingStemmer();
	}

	@Override
	public final Object getIssuer() {

		return this.storage.getServerNetwork().getIssuer();
	}

	@Override
	public final LinkData getLink(final String identifier) {

		this.stsLink++;
		final int index = identifier.hashCode();
		return this.leafs[index & ServerLocal.LEAF_MASK].getLinkData(index & ServerLocal.FOLDERS_MASK, identifier);
	}

	StorageLevel3 getStorage() {

		return this.storage;
	}

	final ExternalHandler getStorageExternalizerReference() {

		return this.storage.getServerInterface().getExternalizerReference();
	}

	@Override
	public final TreeData getTree(final String identifier) {

		this.stsTree++;
		final int index = identifier.hashCode();
		return this.leafs[index & ServerLocal.LEAF_MASK].getTreeData(index & ServerLocal.FOLDERS_MASK, identifier);
	}

	@Override
	public final boolean hasExternal(final Object attachment, final String identifier) throws Exception {

		this.stsHasExternal++;
		final int index = identifier.hashCode();
		return this.leafs[index & ServerLocal.LEAF_MASK].hasExternal(index & ServerLocal.FOLDERS_MASK, identifier) || this.network.hasExternal(attachment, identifier);
	}

	@Override
	public final void invalidateCache() {

		this.cacheTreeSearch.clear();
		this.cacheDictionary.clear();
	}

	@Override
	public final void invalidateDeleteExtra(final String identifier) {

		this.stsUEXTR++;
		final int index = identifier.hashCode();
		this.leafs[index & ServerLocal.LEAF_MASK].invalidateDeleteExtra(index & ServerLocal.FOLDERS_MASK, identifier);
		this.storage.getObjectCache().remove(identifier);
	}

	@Override
	public final void invalidateDeleteLink(final String identifier) {

		this.stsDLINK++;
		final int index = identifier.hashCode();
		this.leafs[index & ServerLocal.LEAF_MASK].invalidateDeleteLink(index & ServerLocal.FOLDERS_MASK, identifier);
		this.cacheTreeSearch.remove(identifier);
		this.storage.invalidateLink(identifier);
	}

	@Override
	public final void invalidateUpdateIdentity(final String identifier) {

		this.stsUIDEN++;
		final int index = identifier.hashCode();
		this.leafs[index & ServerLocal.LEAF_MASK].invalidateUpdateIden(index & ServerLocal.FOLDERS_MASK, identifier);
	}

	@Override
	public final void invalidateUpdateLink(final String identifier) {

		this.stsULINK++;
		final int index = identifier.hashCode();
		this.leafs[index & ServerLocal.LEAF_MASK].invalidateUpdateLink(index & ServerLocal.FOLDERS_MASK, identifier);
		this.storage.invalidateLink(identifier);
	}

	@Override
	public final void invalidateUpdateTree(final String identifier) {

		this.stsUTREE++;
		final int index = identifier.hashCode();
		this.leafs[index & ServerLocal.LEAF_MASK].invalidateUpdateTree(index & ServerLocal.FOLDERS_MASK, identifier);
		this.cacheTreeSearch.remove(identifier);
		this.storage.getObjectCache().remove(identifier);
	}

	@Override
	public final String putExternal(final Object attachment, final String key, final String type, final TransferCopier copier) throws Exception {

		this.stsPutExternal++;
		final String identifier = this.network.putExternal(attachment, key, type, copier);
		final int index = identifier.hashCode();
		this.leafs[index & ServerLocal.LEAF_MASK].putCopier(index & ServerLocal.FOLDERS_MASK, identifier, type, copier);
		return identifier;
	}

	@Override
	public final Map.Entry<String, Object>[]
			search(final String lnkId, final int limit, final boolean all, final long timeout, final String sort, final long dateStart, final long dateEnd, final String filter) {

		this.stsSearch++;
		return this.createSearch.search(this.cacheTreeSearch, lnkId, limit, all, timeout, sort, dateStart, dateEnd, filter);
	}

	@Override
	public final Map<Integer, Map<Integer, Map<String, Number>>>
			searchCalendar(final String lnkId, final boolean all, final long timeout, final long dateStart, final long dateEnd, final String filter) {

		this.stsSearchCalendar++;
		return this.createSearchCalendar.search(this.cacheTreeSearch, lnkId, all, timeout, dateStart, dateEnd, filter);
	}

	@Override
	public final LinkData[] searchIdentity(final String identifier, final boolean all) throws Exception {

		this.stsSearchIdentity++;
		final int index = identifier.hashCode();
		final LinkData[] result = this.leafs[index & ServerLocal.LEAF_MASK].searchIdentity(index & ServerLocal.FOLDERS_MASK, identifier);
		if (all || result == null || result.length == 0) {
			return result;
		}
		int count = 0;
		for (int i = result.length - 1; i >= 0; --i) {
			if (result[i].objState > 0) {
				count++;
			}
		}
		if (count == 0) {
			return null;
		}
		if (count == result.length) {
			return result;
		}
		final LinkData[] filtered = new LinkData[count];
		for (int i = result.length - 1, j = 0; i >= 0; --i) {
			if (result[i].objState > 0) {
				filtered[j++] = result[i];
			}
		}
		return filtered;
	}

	@Override
	public final LinkData[] searchLinks(final String identifier, final boolean all) throws Exception {

		this.stsSearchLinks++;
		final int index = identifier.hashCode();
		final LinkData[] result = this.leafs[index & ServerLocal.LEAF_MASK].searchLinks(index & ServerLocal.FOLDERS_MASK, identifier);
		if (all || result == null || result.length == 0) {
			return result;
		}
		int count = 0;
		for (int i = result.length - 1; i >= 0; --i) {
			if (result[i].objState > 0) {
				count++;
			}
		}
		if (count == 0) {
			return null;
		}
		if (count == result.length) {
			return result;
		}
		final LinkData[] filtered = new LinkData[count];
		for (int i = result.length - 1, j = 0; i >= 0; --i) {
			if (result[i].objState > 0) {
				filtered[j++] = result[i];
			}
		}
		return filtered;
	}

	@Override
	public LinkData[] searchLocal(final String identifier, final String condition) {

		this.stsSearchLocal++;
		final int index = identifier.hashCode();
		return this.leafs[index & ServerLocal.LEAF_MASK].searchLocal(index & ServerLocal.FOLDERS_MASK, identifier, condition);
	}

	@Override
	public final void start() {

		CleanerTask.register(this);
	}

	@Override
	public final void statusFill(final StatusInfo data) {

		final StatusContainer container = new StatusContainer();
		for (int i = ServerLocal.LEAF_MASK; i >= 0; --i) {
			this.leafs[i].statusFill(container);
		}
		data.put("LCL, folder count", Format.Compact.toDecimal(ServerLocal.FOLDERS_COUNT));
		data.put("LCL, common leaf count", Format.Compact.toDecimal(ServerLocal.LEAF_COUNT));
		data.put("LCL, cache size", Format.Compact.toDecimal(container.stsCacheSize));
		data.put("LCL, folder checks", Format.Compact.toDecimal(this.stsChecks));
		data.put("LCL, folder item checks", Format.Compact.toDecimal(container.stsCheckFile));
		data.put("LCL, item-expired checks", Format.Compact.toDecimal(container.stsCheckExpired));
		data.put("LCL, items check requests", Format.Compact.toDecimal(container.stsCheckRequests));
		data.put("LCL, has external", Format.Compact.toDecimal(this.stsHasExternal));
		data.put("LCL, put external", Format.Compact.toDecimal(this.stsPutExternal));
		data.put("LCL, get external", Format.Compact.toDecimal(this.stsExternal));
		data.put("LCL, get external (ref)", Format.Compact.toDecimal(this.stsExternalReference));
		data.put("LCL, read external", Format.Compact.toDecimal(container.stsReadExternal));
		data.put("LCL, write external", Format.Compact.toDecimal(container.stsWriteExternal));
		data.put("LCL, get link", Format.Compact.toDecimal(this.stsLink));
		data.put("LCL, read link", Format.Compact.toDecimal(container.stsReadLink));
		data.put("LCL, read link (expired)", Format.Compact.toDecimal(container.stsReadLinkExpired));
		data.put("LCL, write link", Format.Compact.toDecimal(container.stsWriteLink));
		data.put("LCL, get tree", Format.Compact.toDecimal(this.stsTree));
		data.put("LCL, read tree", Format.Compact.toDecimal(container.stsReadTree));
		data.put("LCL, read tree (expired)", Format.Compact.toDecimal(container.stsReadTreeExpired));
		data.put("LCL, write tree", Format.Compact.toDecimal(container.stsWriteTree));
		data.put("LCL, search identity", Format.Compact.toDecimal(this.stsSearchIdentity));
		data.put("LCL, read search identity", Format.Compact.toDecimal(container.stsReadSearchIdentity));
		data.put("LCL, read search identity (exp)", Format.Compact.toDecimal(container.stsReadSearchIdentityExpired));
		data.put("LCL, write search identity", Format.Compact.toDecimal(container.stsWriteSearchIdentity));
		data.put("LCL, search links", Format.Compact.toDecimal(this.stsSearchLinks));
		data.put("LCL, read search links", Format.Compact.toDecimal(container.stsReadSearchLinks));
		data.put("LCL, read search links (exp)", Format.Compact.toDecimal(container.stsReadSearchLinksExpired));
		data.put("LCL, write search links", Format.Compact.toDecimal(container.stsWriteSearchLinks));
		data.put("LCL, search local", Format.Compact.toDecimal(this.stsSearchLocal));
		data.put("LCL, read search local", Format.Compact.toDecimal(container.stsReadSearchLocal));
		data.put("LCL, read search local (exp)", Format.Compact.toDecimal(container.stsReadSearchLocalExpired));
		data.put("LCL, write search local", Format.Compact.toDecimal(container.stsWriteSearchLocal));
		data.put("LCL, search", Format.Compact.toDecimal(this.stsSearch));
		data.put("LCL, search calendar", Format.Compact.toDecimal(this.stsSearchCalendar));
		data.put("LCL, ULINK", Format.Compact.toDecimal(this.stsULINK));
		data.put("LCL, DLINK", Format.Compact.toDecimal(this.stsDLINK));
		data.put("LCL, UTREE", Format.Compact.toDecimal(this.stsUTREE));
		data.put("LCL, UIDEN", Format.Compact.toDecimal(this.stsUIDEN));
		data.put("LCL, UEXTR", Format.Compact.toDecimal(this.stsUEXTR));
		this.indexingDictionary.statusFill(data);
	}

	@Override
	public final void stop() {

		// ignore
	}

	@Override
	public final String toString() {

		return "S3LOCAL{path=" + this.folderLocal.getAbsolutePath() + "}";
	}
}
