/*
 * Created on 30.06.2004
 */
package ru.myx.xstore.s3;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import ru.myx.ae1.control.AbstractControlEntry;
import ru.myx.ae1.storage.BaseChange;
import ru.myx.ae1.storage.BaseEntry;
import ru.myx.ae1.storage.BaseHistory;
import ru.myx.ae1.storage.BaseSchedule;
import ru.myx.ae1.storage.BaseSync;
import ru.myx.ae1.storage.BaseVersion;
import ru.myx.ae1.storage.ListByMapEntryKey;
import ru.myx.ae1.storage.ModuleSchedule;
import ru.myx.ae1.storage.ModuleSynchronizer;
import ru.myx.ae1.storage.StorageImpl;
import ru.myx.ae1.types.Type;
import ru.myx.ae3.Engine;
import ru.myx.ae3.answer.Reply;
import ru.myx.ae3.answer.ReplyAnswer;
import ru.myx.ae3.base.Base;
import ru.myx.ae3.base.BaseNativeObject;
import ru.myx.ae3.base.BaseObject;
import ru.myx.ae3.base.BasePrimitiveString;
import ru.myx.ae3.base.BaseProperty;
import ru.myx.ae3.control.ControlBasic;
import ru.myx.ae3.control.command.ControlCommand;
import ru.myx.ae3.control.command.ControlCommandset;
import ru.myx.ae3.help.Format;
import ru.myx.ae3.report.Report;
import ru.myx.ae3.serve.ServeRequest;
import ru.myx.xstore.s3.concept.LinkData;
import ru.myx.xstore.s3.concept.ListByLinkData;
import ru.myx.xstore.s3.concept.TreeData;

/** @author myx
 *
 *         To change the template for this generated type comment go to
 *         Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments */
class EntryImpl extends AbstractControlEntry<EntryImpl> implements BaseEntry<EntryImpl> {

	final StorageLevel3 storage;

	private final String lnkId;

	private LinkData linkData = null;

	private TreeData treeData = null;

	private EntryImpl cachedParent = null;

	EntryImpl(final StorageLevel3 storage, final LinkData link) {

		this.storage = storage;
		this.linkData = link;
		this.lnkId = link.lnkId;
	}

	@Override
	public BaseProperty baseGetOwnProperty(final BasePrimitiveString name) {

		{
			final BaseObject substitution = this.getData();
			assert substitution != null : "Should not be NULL, class=" + this.getClass().getName();
			assert substitution != this : "Should not be THIS, class=" + this.getClass().getName();
			final BaseProperty property = substitution.baseFindProperty(name);
			if (property != null) {
				return property;
			}
		}
		{
			final Type<?> type = this.getType();
			return type == null
				? null
				: type.getTypePrototypeObject().baseFindProperty(name, BaseObject.PROTOTYPE);
		}
	}

	@Override
	public BaseProperty baseGetOwnProperty(final String name) {

		{
			final BaseObject substitution = this.getData();
			assert substitution != null : "Should not be NULL, class=" + this.getClass().getName();
			assert substitution != this : "Should not be THIS, class=" + this.getClass().getName();
			final BaseProperty property = substitution.baseFindProperty(name, BaseObject.PROTOTYPE);
			if (property != null) {
				return property;
			}
		}
		{
			final Type<?> type = this.getType();
			return type == null
				? null
				: type.getTypePrototypeObject().baseFindProperty(name, BaseObject.PROTOTYPE);
		}
	}

	@Override
	public final BaseObject baseGetSubstitution() {

		return this.getData();
	}

	@Override
	public boolean baseHasKeysOwn() {

		/** faster - no need to load */
		{
			final Type<?> type = this.getType();
			if (type != null) {
				final BaseObject typePrototype = type.getTypePrototypeObject();
				if (typePrototype != null
						/** not only OWN */
						&& Base.hasKeys(typePrototype)) {
					return true;
				}
			}
		}
		{
			final BaseObject substitution = this.getData();
			assert substitution != null : "Should not be NULL, class=" + this.getClass().getName();
			assert substitution != this : "Should not be THIS, class=" + this.getClass().getName();
			return Base.hasKeys(substitution);
		}
	}

	@Override
	public BaseObject basePrototype() {

		return BaseEntry.PROTOTYPE;
	}

	@Override
	public final int compareTo(final Object o) {

		return this.getGuid().compareTo(((BaseEntry<?>) o).getGuid());
	}

	@Override
	public BaseChange createChange() {

		this.storage.stsChangesCreated++;
		return new ChangeForEntry(this.storage, this);
	}

	@Override
	public BaseChange createChild() {

		this.storage.stsChangesCreated++;
		return new ChangeForNew(this.storage, this.lnkId, Engine.createGuid(), this);
	}

	@Override
	public final boolean equals(final Object o) {

		return o == this || o instanceof BaseEntry<?> && this.getGuid().equals(((BaseEntry<?>) o).getGuid());
	}

	@Override
	public String[] getAliases() {

		return this.storage.getServerNetwork().getAliases(this.lnkId);
	}

	@Override
	public BaseEntry<?> getChildByName(final String name) {

		final String guid = this.getTree().getChildGuid(name);
		if (guid == null) {
			return null;
		}
		{
			final BaseEntry<?> child = this.storage.getByGuid(guid);
			if (child != null) {
				return child;
			}
		}
		{
			this.storage.getServerInterface().invalidateUpdateLink(guid);

			/** Try again - race condition check */
			final String check2 = this.getTree().getChildGuid(name);
			if (check2 == null) {
				return null;
			}
			if (check2.equals(guid)) {
				this.storage.getServerInterface().invalidateUpdateTree(this.lnkId);

				final BaseEntry<?> child = this.storage.getByGuid(guid);
				if (child != null) {
					Report.event(
							"S3ENTRY",
							"Child in tree but not accessible, missing, patched, guid=" + guid + ", container=" + this.lnkId,
							Format.Throwable.toText(new RuntimeException(guid)));
					return child;
				}
				{
					Report.event(
							"S3ENTRY",
							"Child in tree but not accessible, missing, lost, guid=" + guid + ", container=" + this.lnkId,
							Format.Throwable.toText(new RuntimeException(guid)));
					return null;
				}
			}
			{
				final BaseEntry<?> child = this.storage.getByGuid(check2);
				if (child != null) {
					Report.event(
							"S3ENTRY",
							"Child in tree but not accessible, changed, patched, guid=" + guid + ", newGuid=" + check2 + ", container=" + this.lnkId,
							Format.Throwable.toText(new RuntimeException(guid)));
					return child;
				}
				{
					Report.event(
							"S3ENTRY",
							"Child in tree but not accessible, changed, lost, guid=" + guid + ", newGuid=" + check2 + ", container=" + this.lnkId,
							Format.Throwable.toText(new RuntimeException(guid)));
					return null;
				}
			}
		}
	}

	@Override
	public ListByLinkData<ControlBasic<?>> getChildren() {

		final Type<?> type = this.getType();
		final LinkData[] result = this.getTree().getChildren(
				this.storage,
				this.getLink(),
				0,
				type == null
					? null
					: type.getTypeBehaviorListingSort());
		return result == null || result.length == 0
			? null
			: new ListByLinkData<>(result, this.storage);
	}

	@Override
	public ListByLinkData<ControlBasic<?>> getChildren(final int count, final String sort) {

		final LinkData[] result = this.getTree().getChildren(this.storage, this.getLink(), count, sort);
		return result == null || result.length == 0
			? null
			: new ListByLinkData<>(result, this.storage);
	}

	@Override
	public ListByLinkData<ControlBasic<?>> getChildrenListable(final int count, final String sort) {

		final LinkData[] result = this.getTree().getChildrenListable(this.storage, this.getLink(), count, sort);
		return result == null || result.length == 0
			? null
			: new ListByLinkData<>(result, this.storage);
	}

	@Override
	public final Object getCommandResult(final ControlCommand<?> command, final BaseObject arguments) {

		return this.getType().getCommandAdditionalResult(this, command, arguments);
	}

	@Override
	public final ControlCommandset getCommands() {

		final Type<?> type = this.getType();
		if (type == null) {
			return null;
		}
		return type.getCommandsAdditional(this, null, null, null);
	}

	@Override
	public long getCreated() {

		return this.getLink().objCreated;
	}

	@Override
	public BaseObject getData() {

		return this.getLink().getData(this.storage, null);
	}

	BaseObject getDataClean() {

		final BaseObject result = new BaseNativeObject();
		final BaseObject map = this.getLink().getDataReal(this.storage, null);
		if (map != null) {
			result.baseDefineImportAllEnumerable(map);
		}
		return result;
	}

	@Override
	public String getEntryBehaviorListingSort() {

		final Type<?> type = this.getType();
		return type == null
			? null
			: type.getTypeBehaviorListingSort();
	}

	@Override
	public ListByLinkData<ControlBasic<?>> getFiles(final int count, final String sort) {

		final LinkData[] result = this.getTree().getFiles(this.storage, this.getLink(), count, sort);
		return result == null || result.length == 0
			? null
			: new ListByLinkData<>(result, this.storage);
	}

	@Override
	public ListByLinkData<ControlBasic<?>> getFilesListable(final int count, final String sort) {

		final LinkData[] result = this.getTree().getFilesListable(this.storage, this.getLink(), count, sort);
		return result == null || result.length == 0
			? null
			: new ListByLinkData<>(result, this.storage);
	}

	@Override
	public ListByLinkData<ControlBasic<?>> getFolders() {

		final LinkData[] result = this.getTree().getFolders();
		return result == null || result.length == 0
			? null
			: new ListByLinkData<>(result, this.storage);
	}

	@Override
	public ListByLinkData<ControlBasic<?>> getFoldersListable() {

		final LinkData[] result = this.getTree().getFoldersListable();
		return result == null || result.length == 0
			? null
			: new ListByLinkData<>(result, this.storage);
	}

	@Override
	public String getGuid() {

		return this.lnkId;
	}

	@Override
	public BaseHistory[] getHistory() {

		return this.storage.getServerNetwork().getObjectHistory(this.getLink().objId);
	}

	@Override
	public BaseEntry<?> getHistorySnapshot(final String historyId) {

		final LinkData linkHistory = this.storage.getServerNetwork().getObjectHistorySnapshot(this.lnkId, historyId);
		return linkHistory == null
			? null
			: new EntryImplHistory(this.storage, linkHistory, historyId);
	}

	@Override
	public final String getIcon() {

		final Type<?> type = this.getType();
		final String typeIcon = type == null
			? "unknown"
			: type.getIcon();
		return this.isFolder()
			? "container-" + typeIcon
			: "object-" + typeIcon;
	}

	@Override
	public String getKey() {

		return this.getLink().lnkName;
	}

	final LinkData getLink() {

		final LinkData link = this.linkData;
		if (link != null && !link.invalid) {
			return link;
		}
		final LinkData replacement = this.storage.getServerInterface().getLink(this.lnkId);
		if (replacement == null) {
			if (link == null) {
				throw new IllegalStateException("Object is deleted? (Link is unknown for entry)");
			}
			Report.warning("S3ENTRY", "Link was deleted while this entry is loaded and used: guid=" + this.lnkId);
			return link;
		}
		return this.linkData = replacement;
	}

	@Override
	public String getLinkedIdentity() {

		return this.getLink().objId;
	}

	@Override
	public final String getLocationControl() {

		final StorageImpl parent = this.getStorageImpl();
		if (this.getGuid().equals(parent.getStorage().getRootIdentifier())) {
			return parent.getLocationControl();
		}
		final BaseEntry<?> entry = this.getParent();
		if (entry == null || entry == this) {
			return null;
		}
		final String parentPath = entry.getLocationControl();
		if (parentPath == null) {
			return null;
		}
		return parentPath.endsWith("/")
			? this.isFolder()
				? parentPath + this.getKey() + '/'
				: parentPath + this.getKey()
			: this.isFolder()
				? parentPath + '/' + this.getKey() + '/'
				: parentPath + '/' + this.getKey();
	}

	@Override
	public final Collection<String> getLocationControlAll() {

		final StorageImpl parent = this.getStorageImpl();
		if (!parent.areLinksSupported()) {
			return Collections.singleton(this.getLocationControl());
		}
		final Collection<ControlBasic<?>> same = parent.getInterface().searchForIdentity(this.getLinkedIdentity(), true);
		if (same == null) {
			return Collections.singleton(this.getLocationControl());
		}
		final List<String> result = new ArrayList<>();
		for (final ControlBasic<?> current : same) {
			if (current != null) {
				final BaseEntry<?> entry = (BaseEntry<?>) current;
				result.add(entry.getLocationControl());
			}
		}
		return result;
	}

	@Override
	public long getModified() {

		return this.getLink().objModified;
	}

	@Override
	public String getOwner() {

		return this.getLink().objOwner;
	}

	@Override
	public final BaseEntry<?> getParent() {

		final EntryImpl cachedParent = this.cachedParent;
		return cachedParent != null
			? cachedParent
			: (this.cachedParent = this.storage.getByGuid(this.getParentGuid()));
	}

	@Override
	public String getParentGuid() {

		return this.getLink().lnkCntId;
	}

	@Override
	public BaseSchedule getSchedule() {

		final ModuleSchedule scheduling = this.getStorageImpl().getScheduling();
		return scheduling == null
			? null
			: scheduling.createChange(this.getGuid());
	}

	@Override
	public int getState() {

		return this.getLink().objState;
	}

	@Override
	public StorageImpl getStorageImpl() {

		return this.storage;
	}

	@Override
	public BaseSync getSynchronization() {

		final ModuleSynchronizer synchronizer = this.getStorageImpl().getSynchronizer();
		return synchronizer == null
			? null
			: synchronizer.createChange(this.getGuid());
	}

	@Override
	public String getTitle() {

		return this.getLink().objTitle;
	}

	final TreeData getTree() {

		final TreeData tree = this.treeData;
		if (tree != null && !tree.invalid) {
			return tree;
		}
		final TreeData replacement = this.storage.getServerInterface().getTree(this.lnkId);
		if (replacement == null) {
			if (tree == null) {
				throw new IllegalStateException("Tree is unknown for entry, entry=" + this);
			}
			Report.warning("S3ENTRY", "Tree was deleted while this entry is loaded and used: guid=" + this.lnkId);
			return tree;
		}
		return this.treeData = replacement;
	}

	@Override
	public Type<?> getType() {

		return this.storage.getServer().getTypes().getType(this.getLink().objType);
	}

	@Override
	public String getTypeName() {

		return this.getLink().objType;
	}

	@Override
	public BaseEntry<?> getVersion(final String versionId) {

		final LinkData linkVersion = this.storage.getServerNetwork().getObjectVersion(this.lnkId, this.getLink().objId, versionId);
		return linkVersion == null
			? null
			: new EntryImplVersion(this.storage, this, linkVersion, versionId);
	}

	String getVersionId() {

		return this.getLink().vrId;
	}

	/** @return result */
	public final boolean getVersioning() {

		return !"*".equals(this.getLink().vrId);
	}

	@Override
	public BaseVersion[] getVersions() {

		return this.storage.getServerNetwork().getObjectVersions(this.getLink().objId);
	}

	@Override
	public final int hashCode() {

		return this.getGuid().hashCode();
	}

	public void invalidateLink() {

		if (this.linkData != null) {
			this.linkData.invalidateThis();
			this.linkData = null;
		}
		this.cachedParent = null;
	}

	@Override
	public boolean isFolder() {

		return this.getLink().lnkFolder;
	}

	@Override
	public ReplyAnswer onQuery(final ServeRequest query) {

		final Type<?> type = this.getType();
		return type == null
			? Reply.string("BASE_ENTRY", query, "NULL TYPE: " + this.getTypeName())
			: type.getResponse(query, this);
	}

	@Override
	public final String restoreFactoryIdentity() {

		return "storage";
	}

	@Override
	public final String restoreFactoryParameter() {

		return this.getStorageImpl().getMnemonicName() + ',' + this.getLinkedIdentity();
	}

	@Override
	public List<ControlBasic<?>> search(final int limit, final boolean all, final long timeout, final String sort, final long dateStart, final long dateEnd, final String filter) {

		if (limit == 0L && timeout == 0L) {
			final Map.Entry<String, Object>[] result = this.storage.getServerNetwork().search(this.lnkId, limit, all, timeout, sort, dateStart, dateEnd, filter);
			return result == null || result.length == 0
				? null
				: new ListByMapEntryKey<>(result, this.storage);
		}
		final Map.Entry<String, Object>[] result = this.storage.getServerInterface().search(this.lnkId, limit, all, timeout, sort, dateStart, dateEnd, filter);
		return result == null || result.length == 0
			? null
			: new ListByMapEntryKey<>(result, this.storage);
	}

	@Override
	public Map<Integer, Map<Integer, Map<String, Number>>> searchCalendar(final boolean all, final long timeout, final long startDate, final long endDate, final String filter) {

		return this.storage.getServerInterface().searchCalendar(this.lnkId, all, timeout, startDate, endDate, filter);
	}

	@Override
	public ListByLinkData<ControlBasic<?>> searchLocal(final int limit, final boolean all, final String sort, final long startDate, final long endDate, final String query) {

		final LinkData[] result = this.getTree().searchLocal(this.storage, limit, all, sort, startDate, endDate, query);
		return result == null || result.length == 0
			? null
			: new ListByLinkData<>(result, this.storage);
	}

	@Override
	public Map<String, Number> searchLocalAlphabet(final boolean all, final Map<String, String> alphabetConversion, final String defaultLetter, final String filter) {

		return this.getTree().searchLocalAlphabetStats(this.storage, all, alphabetConversion, defaultLetter, filter);
	}

	@Override
	public ListByLinkData<ControlBasic<?>> searchLocalAlphabet(final int limit,
			final boolean all,
			final String sort,
			final Map<String, String> alphabetConversion,
			final String defaultLetter,
			final String filterLetter,
			final String query) {

		final LinkData[] result = this.getTree().searchLocalAlphabet(this.storage, limit, all, sort, alphabetConversion, defaultLetter, filterLetter, query);
		return result == null || result.length == 0
			? null
			: new ListByLinkData<>(result, this.storage);
	}

	@Override
	public Map<Integer, Map<Integer, Map<String, Number>>> searchLocalCalendar(final boolean all, final long startDate, final long endDate, final String query) {

		return this.getTree().searchLocalCalendarStats(this.storage, all, startDate, endDate, query);
	}

	@Override
	public String toString() {

		return "[object " + this.baseClass() + "(" + this.toStringDetails() + ")]";
	}

	@Override
	protected String toStringDetails() {

		final LinkData link = this.getLink();
		return link == null
			? "id=" + this.lnkId + ", dead."
			: "id=" + this.lnkId + ", key=" + link.lnkName + ", typeName=" + link.objType;
	}
}
