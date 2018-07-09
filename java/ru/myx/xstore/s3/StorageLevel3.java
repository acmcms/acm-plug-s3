/*
 * Created on 27.06.2004
 */
package ru.myx.xstore.s3;

import java.io.File;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import ru.myx.ae1.AbstractPluginInstance;
import ru.myx.ae1.control.Control;
import ru.myx.ae1.control.MultivariantString;
import ru.myx.ae1.know.Server;
import ru.myx.ae1.provide.ProvideStatus;
import ru.myx.ae1.storage.BaseChange;
import ru.myx.ae1.storage.BaseEntry;
import ru.myx.ae1.storage.BaseRecycled;
import ru.myx.ae1.storage.ModuleInterface;
import ru.myx.ae1.storage.ModuleRecycler;
import ru.myx.ae1.storage.ModuleSchedule;
import ru.myx.ae1.storage.ModuleSynchronizer;
import ru.myx.ae1.storage.PluginRegistry;
import ru.myx.ae1.storage.StorageImpl;
import ru.myx.ae2.indexing.IndexingDictionary;
import ru.myx.ae2.indexing.IndexingStemmer;
import ru.myx.ae3.Engine;
import ru.myx.ae3.base.Base;
import ru.myx.ae3.base.BaseNativeObject;
import ru.myx.ae3.base.BaseObject;
import ru.myx.ae3.base.BaseProperty;
import ru.myx.ae3.cache.Cache;
import ru.myx.ae3.cache.CacheL1;
import ru.myx.ae3.cache.CacheL2;
import ru.myx.ae3.cache.CacheL3;
import ru.myx.ae3.cache.CacheType;
import ru.myx.ae3.cache.CreationHandlerObject;
import ru.myx.ae3.control.ControlBasic;
import ru.myx.ae3.control.command.ControlCommand;
import ru.myx.ae3.control.command.ControlCommandset;
import ru.myx.ae3.help.Convert;
import ru.myx.ae3.help.Format;
import ru.myx.ae3.report.Report;
import ru.myx.ae3.status.StatusInfo;
import ru.myx.ae3.status.StatusRegistry;
import ru.myx.jdbc.lock.Interest;
import ru.myx.jdbc.lock.LockManager;
import ru.myx.jdbc.lock.Locker;
import ru.myx.xstore.basics.ControlNodeImpl;
import ru.myx.xstore.basics.StorageSynchronizationActorProvider;
import ru.myx.xstore.s3.concept.LinkData;
import ru.myx.xstore.s3.concept.ListByLinkData;
import ru.myx.xstore.s3.concept.StorageInterface;
import ru.myx.xstore.s3.concept.StorageNetwork;
import ru.myx.xstore.s3.jdbc.ServerJdbc;
import ru.myx.xstore.s3.local.ServerLocal;

/** @author myx */
public final class StorageLevel3 extends AbstractPluginInstance
		implements
			StorageImpl,
			ModuleInterface,
			ModuleRecycler,
			LockManager,
			CreationHandlerObject<StorageLevel3, EntryImpl> {

	private static final ControlCommand<?> CMD_CLEAR_CACHES = Control.createCommand(//
			"clear_cache",
			MultivariantString.getString(//
					"Clear storage caches",
					Collections.singletonMap("ru", "Очистить кеши хранилища")//
			))//
			.setCommandPermission("publish")//
			.setCommandIcon("command-dispose")//
	;

	private static final ControlCommand<?> CMD_CLEAR_TEMPLATE_CACHES = Control.createCommand(//
			"clear_template_cache",
			MultivariantString.getString(//
					"Clear template caches",
					Collections.singletonMap("ru", "Очистить кеши шаблонов")//
			))//
			.setCommandPermission("publish")//
			.setCommandIcon("command-dispose")//
	;

	// /////////////////////////////////////////////////////////////////////////////////////////////////////////
	// /////////////////////////////////////////////////////////////////////////////////////////////////////////
	// /////////////////////////////////////////////////////////////////////////////////////////////////////////
	// /////////////////////////////////////////////////////////////////////////////////////////////////////////
	//
	//
	// Plugin methods
	//
	//
	private StorageNetwork serverNetwork;

	private StorageInterface serverInterface;

	private String storageId;

	private String interfaceId;

	private String connection;

	private boolean scheduling;

	private File cacheRoot;

	private String storageIdentity;

	private Enumeration<Connection> connectionSource;

	private boolean client;

	int stsChangesCreated = 0;

	int stsChangesNested = 0;

	private String controlLocation;

	private long presenceValidFrom = 0;

	private final CacheL1<EntryImpl> cache = Cache.createL1("s3entryImpl", CacheType.NORMAL_JAVA_WEAK);

	@Override
	public final boolean addInterest(final Interest interest) {

		return this.serverNetwork.addInterest(interest);
	}

	@Override
	public final BaseEntry<?> apply(final String key) {

		return key == null
			? null
			: this.cache.getCreate(key, this, key, this);
	}

	@Override
	public final boolean areAliasesSupported() {

		return true;
	}

	@Override
	public final boolean areHistoriesSupported() {

		return true;
	}

	@Override
	public final boolean areLinksSupported() {

		return true;
	}

	@Override
	public final boolean areLocksSupported() {

		return true;
	}

	@Override
	public final boolean areObjectHistoriesSupported() {

		return true;
	}

	@Override
	public final boolean areObjectVersionsSupported() {

		return true;
	}

	@Override
	public final boolean areSchedulesSupported() {

		return true;
	}

	@Override
	public final boolean areSoftDeletionsSupported() {

		return true;
	}

	@Override
	public final boolean areSynchronizationsSupported() {

		return this.serverNetwork.areSynchronizationsSupported();
	}

	@Override
	public final boolean areVersionsSupported() {

		return true;
	}

	@Override
	public final void clearAllRecycled() {

		this.serverNetwork.clearAllRecycled();
	}

	final Object clearCacheAll() {

		this.serverInterface.invalidateCache();
		this.getServer().getObjectCache().clear();
		return MultivariantString.getString("All storage caches were discarded.", Collections.singletonMap("ru", "Кеши хранилища успешно очищены."));
	}

	final Object clearCacheTypes() {

		this.getServer().getObjectCache().clear();
		return MultivariantString.getString("All template caches were discarded.", Collections.singletonMap("ru", "Кеши шаблонов успешно очищены."));
	}

	@Override
	public EntryImpl create(final StorageLevel3 attachment, final String key) {

		final LinkData link = this.serverInterface.getLink(key);
		return link == null
			? null
			: new EntryImpl(this, link);
	}

	@Override
	public final Locker createLock(final String guid, final int version) {

		return this.serverNetwork.createLock(guid, version);
	}

	@Override
	public final void destroy() {

		PluginRegistry.removePlugin(this);
		this.serverInterface.stop();
		this.serverNetwork.stop();
	}

	@Override
	public final BaseEntry<?> getByAlias(final String alias, final boolean all) {

		final String key;
		try {
			final LinkData[] guids = this.serverInterface.searchIdentity(alias, all);
			if (guids == null || guids.length == 0) {
				return null;
			}
			key = guids[0].lnkId;
		} catch (final RuntimeException e) {
			throw e;
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
		return this.cache.getCreate(key, this, key, this);
	}

	@Override
	public final EntryImpl getByGuid(final String key) {

		return key == null
			? null
			: this.cache.getCreate(key, this, key, this);
	}

	@Override
	public final BaseEntry<?> getByGuidClean(final String key, final String typeName) {

		try {
			final LinkData link = this.serverNetwork.getLink(key);
			if (link == null) {
				return null;
			}
			return new EntryImplTypeOverride(this, link, typeName);
		} catch (final RuntimeException e) {
			throw e;
		} catch (final Exception t) {
			throw new RuntimeException(t);
		}
	}

	@Override
	public final Object getCommandResult(final ControlCommand<?> command, final BaseObject arguments) {

		if (command == StorageLevel3.CMD_CLEAR_CACHES) {
			{
				final BaseObject result = new BaseNativeObject()//
						.putAppend("storage", this.getMnemonicName());
				this.getServer().logQuickTaskUsage("XDS_COMMAND_STORAGE(3)_CC", result);
			}
			return this.clearCacheAll();
		}
		if (command == StorageLevel3.CMD_CLEAR_TEMPLATE_CACHES) {
			{
				final BaseObject result = new BaseNativeObject()//
						.putAppend("storage", this.getMnemonicName());
				this.getServer().logQuickTaskUsage("XDS_COMMAND_STORAGE(3)_CT", result);
			}
			return this.clearCacheTypes();
		}
		throw new IllegalArgumentException("Unknown command: " + command.getKey());
	}

	@Override
	public ControlCommandset getCommands() {

		final ControlCommandset result = Control.createOptions();
		result.add(StorageLevel3.CMD_CLEAR_CACHES);
		result.add(StorageLevel3.CMD_CLEAR_TEMPLATE_CACHES);
		return result;
	}

	@Override
	public final Enumeration<Connection> getConnectionSource() {

		return this.connectionSource;
	}

	@Override
	public final ControlCommandset getContentCommands(final String key) {

		return null;
	}

	/** @return identity */
	public final String getIdentity() {

		return this.storageIdentity;
	}

	// /////////////////////////////////////////////////////////////////////////////////////////////////////////
	// /////////////////////////////////////////////////////////////////////////////////////////////////////////
	// /////////////////////////////////////////////////////////////////////////////////////////////////////////
	// /////////////////////////////////////////////////////////////////////////////////////////////////////////
	//
	//
	// Impl methods
	//
	//
	/** @return result */
	public final IndexingDictionary getIndexingDictionary() {

		return this.serverInterface.getIndexingDictionary();
	}

	/** @return result */
	public final IndexingStemmer getIndexingStemmer() {

		return this.serverInterface.getIndexingStemmer();
	}

	@Override
	public final ModuleInterface getInterface() {

		return this;
	}

	@Override
	public String getLocationControl() {

		return this.controlLocation;
	}

	@Override
	public final LockManager getLocker() {

		return this;
	}

	@Override
	public final String getMnemonicName() {

		return this.storageId;
	}

	@Override
	public final <T> CacheL2<T> getObjectCache() {

		return this.getServer().getObjectCache();
	}

	/** @return */
	public long getPresenceValidityDate() {

		return this.presenceValidFrom;
	}

	@Override
	public final BaseRecycled[] getRecycled() {

		return this.serverNetwork.getRecycled();
	}

	@Override
	public final BaseRecycled getRecycledByGuid(final String guid) {

		return this.serverNetwork.getRecycledByGuid(guid);
	}

	@Override
	public final ModuleRecycler getRecycler() {

		return this;
	}

	@Override
	public final BaseEntry<?> getRoot() {

		return this.getByGuid(this.getRootIdentifier());
	}

	@Override
	public final String getRootIdentifier() {

		return "$$ROOT_ENTRY";
	}

	@Override
	public final ModuleSchedule getScheduling() {

		return this.serverNetwork.getScheduling();
	}

	/** @return server */
	public final StorageInterface getServerInterface() {

		return this.serverInterface;
	}

	/** @return server */
	public final StorageNetwork getServerNetwork() {

		return this.serverNetwork;
	}

	@Override
	public final ModuleInterface getStorage() {

		return this;
	}

	@Override
	public final ModuleSynchronizer getSynchronizer() {

		return this.serverNetwork.getSynchronizer();
	}

	@Override
	public long getTTL() {

		return 1000 * 60 * 60 * 24 * 7L;
	}

	/** @param guid
	 */
	public void invalidateLink(final String guid) {

		final EntryImpl entry = this.cache.get(guid);
		if (entry != null) {
			entry.invalidateLink();
		}
		this.getObjectCache().remove(guid);
	}

	@Override
	public final Connection nextConnection() {

		return this.connectionSource.nextElement();
	}

	@Override
	public final void register() {

		final Server server = this.getServer();
		this.connectionSource = server.getConnections().get(this.connection);
		final CacheL3<Object> cache = server.getCache();
		final String cachePrefix = server.getZoneId() + '-' + this.storageId;
		{
			String storageIdentity = "";
			final BaseObject settings = this.getSettingsPrivate();
			storageIdentity = Base.getString(settings, "identity", "").trim();
			if (storageIdentity.length() == 0) {
				storageIdentity = Engine.createGuid();
				settings.baseDefine("identity", storageIdentity);
			}
			this.storageIdentity = storageIdentity;
			this.presenceValidFrom = Convert.MapEntry.toLong(settings, "presenceValidFrom", this.presenceValidFrom);
			this.commitPrivateSettings();
		}
		try {
			this.cacheRoot = new File(new File(Engine.PATH_CACHE, server.getZoneId()), this.storageId);
			this.cacheRoot.mkdirs();
		} catch (final RuntimeException e) {
			throw e;
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
		this.serverNetwork = new ServerJdbc(this, this.storageIdentity, this.scheduling, this.client);
		this.serverInterface = new ServerLocal(this, this.cacheRoot, cache, cachePrefix);
		this.controlLocation = '/' + this.getMnemonicName() + '/';
		server.getControlRoot().bind(new ControlNodeImpl(this, this.getStorage().getRootIdentifier(), true));
		server.registerCommonActor(new StorageSynchronizationActorProvider(server, this.controlLocation));
		final Map<String, Function<Void, Object>> registrySignals = server.registrySignals();
		registrySignals.put("S3_CLEAR_CACHE." + this.getMnemonicName(), new SignalCommand(this, StorageLevel3.CMD_CLEAR_CACHES));
		registrySignals.put("S3_REFRESH_TYPES." + this.getMnemonicName(), new SignalCommand(this, StorageLevel3.CMD_CLEAR_TEMPLATE_CACHES));
		((StatusRegistry) server.getRootContext().baseGet(ProvideStatus.REGISTRY_CONTEXT_KEY, BaseObject.UNDEFINED).baseValue()).register(new Status(this));
	}

	@Override
	public final boolean removeInterest(final Interest interest) {

		return this.serverNetwork.removeInterest(interest);
	}

	@Override
	public final Collection<ControlBasic<?>> searchForIdentity(final String guid, final boolean all) {

		try {
			final LinkData[] links = this.serverInterface.searchIdentity(guid, all);
			if (links == null || links.length == 0) {
				return null;
			}
			if (links.length == 1) {
				final List<ControlBasic<?>> result = new ArrayList<>(1);
				result.add(this.getByGuid(links[0].lnkId));
				return result;
			}
			return new ListByLinkData<>(links, this);
		} catch (final RuntimeException e) {
			throw e;
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	/** @param validFrom
	 */
	public void setPresenceValidityDate(final long validFrom) {

		this.presenceValidFrom = Math.max(validFrom, this.presenceValidFrom);
	}

	@Override
	public final void setup() {

		final BaseObject info = this.getSettingsProtected();
		this.storageId = Base.getString(info, "id", "s3");
		this.interfaceId = Base.getString(info, "api", "s3");
		this.connection = Base.getString(info, "connection", "default");
		this.client = Convert.MapEntry.toBoolean(info, "client", false);
		this.scheduling = !this.client && Convert.MapEntry.toBoolean(info, "scheduling", true);
	}

	@Override
	public final void start() {

		final BaseObject reflection = this.getServer().getRootContext().ri10GV;
		final BaseObject baseStorage = Base.forUnknown(this);
		reflection.baseDefine(this.interfaceId, baseStorage, BaseProperty.ATTRS_MASK_NNN);
		reflection.baseDefine(this.storageId, baseStorage, BaseProperty.ATTRS_MASK_NNN);
		if (reflection.baseGetOwnProperty("Storage") == null) {
			reflection.baseDefine("Storage", baseStorage, BaseProperty.ATTRS_MASK_NNN);
		}
		this.serverNetwork.start();
		this.serverInterface.start();
		{
			final String rootId = this.getRootIdentifier();
			final BaseEntry<?> root = this.getByGuid(rootId);
			if (root == null) {
				Report.info("S3/STORAGE", "No root (id=" + rootId + ") entry found, trying to create...");
				final BaseChange change = new ChangeForNew(this, "*", rootId, null);
				change.setKey("root");
				change.setTypeName("SystemRoot");
				change.setTitle("Storage: " + this.getMnemonicName());
				change.setState(ModuleInterface.STATE_READY);
				change.setFolder(true);
				change.commit();
				this.serverInterface.invalidateCache();
				Report.info("S3/STORAGE", "No root entry found (id=" + rootId + "), seems to be created, check=" + this.getByGuid(rootId));
			}
		}
		PluginRegistry.addPlugin(this);
	}

	@Override
	public final void start(final String identity) {

		this.serverNetwork.start(identity);
	}

	final void statusFill(final StatusInfo data) {

		data.put("API, changes created", Format.Compact.toDecimal(this.stsChangesCreated));
		data.put("API, nested changes", Format.Compact.toDecimal(this.stsChangesNested));
		this.serverInterface.statusFill(data);
		this.serverNetwork.statusFill(data);
	}

	@Override
	public final void stop(final String identity) {

		this.serverNetwork.stop(identity);
	}

	@Override
	public final String toString() {

		return "S3(zone:" + this.getServer().getZoneId() + ", name:" + this.getMnemonicName() + ')';
	}
}
