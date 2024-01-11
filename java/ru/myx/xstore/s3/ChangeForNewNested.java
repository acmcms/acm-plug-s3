/*
 * Created on 04.07.2004
 */
package ru.myx.xstore.s3;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import ru.myx.ae1.storage.AbstractSchedule;
import ru.myx.ae1.storage.AbstractSync;
import ru.myx.ae1.storage.BaseChange;
import ru.myx.ae1.storage.BaseEntry;
import ru.myx.ae1.storage.BaseSchedule;
import ru.myx.ae1.storage.BaseSync;
import ru.myx.ae1.storage.ModuleInterface;
import ru.myx.ae1.storage.StorageImpl;
import ru.myx.ae1.types.Type;
import ru.myx.ae3.Engine;
import ru.myx.ae3.act.Context;
import ru.myx.ae3.base.Base;
import ru.myx.ae3.base.BaseNativeObject;
import ru.myx.ae3.base.BaseObject;
import ru.myx.ae3.exec.Exec;
import ru.myx.ae3.help.Create;
import ru.myx.xstore.s3.concept.Transaction;

/** @author myx
 *
 *         To change the template for this generated type comment go to
 *         Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments */
class ChangeForNewNested extends ChangeAbstract implements ChangeNested {

	private Set<String> aliasAdd = null;

	private final BaseChange change;

	BaseSchedule changeSchedule = null;

	BaseSync changeSync = null;

	private List<ChangeNested> children = null;

	private final BaseObject data = new BaseNativeObject();

	private final String guid;

	private BaseEntry<?> linkedWith = null;

	private boolean local = false;

	private final StorageLevel3 storage;

	private String parentGuid;

	private final List<ChangeNested> target;

	private String versionId = null;

	private Set<String> linkedIn = null;

	ChangeForNewNested(final BaseChange change, final List<ChangeNested> target, final StorageLevel3 parent, final String guid) {

		this.change = change;
		this.target = target;
		this.storage = parent;
		this.parentGuid = change.getGuid();
		this.guid = guid;
		parent.stsChangesNested++;
	}

	@Override
	public void aliasAdd(final String alias) {

		if (this.aliasAdd == null) {
			this.aliasAdd = Create.tempSet();
		}
		this.aliasAdd.add(alias);
	}

	@Override
	public void aliasRemove(final String alias) {

		if (this.aliasAdd != null) {
			this.aliasAdd.remove(alias);
		}
	}

	@Override
	public void commit() {

		this.target.add(this);
	}

	@Override
	public BaseChange createChange(final BaseEntry<?> entry) {

		if (entry == null) {
			return null;
		}
		if (entry.getClass() != EntryImpl.class || entry.getStorageImpl() != this.storage) {
			return entry.createChange();
		}
		if (this.children == null) {
			synchronized (this) {
				if (this.children == null) {
					this.children = new ArrayList<>();
				}
			}
		}
		return new ChangeForEntryNested(this.children, this.storage, (EntryImpl) entry);
	}

	@Override
	public BaseChange createChild() {

		if (this.children == null) {
			synchronized (this) {
				if (this.children == null) {
					this.children = new ArrayList<>();
				}
			}
		}
		return new ChangeForNewNested(this, this.children, this.storage, Engine.createGuid());
	}

	@Override
	public final void delete() {

		this.delete(false);
	}

	@Override
	public final void delete(final boolean soft) {

		throw new UnsupportedOperationException("'delete' is unsupported on non-commited entries!");
	}

	@Override
	public BaseObject getData() {

		return this.data;
	}

	@Override
	public String getGuid() {

		return this.guid;
	}

	@Override
	public String getLinkedIdentity() {

		return null;
	}

	@Override
	public final String getLocationControl() {

		final StorageImpl parent = this.getStorageImpl();
		if (this.getGuid().equals(parent.getStorage().getRootIdentifier())) {
			return parent.getLocationControl();
		}
		if (this.change == null) {
			return null;
		}
		final String parentPath = this.change.getLocationControl();
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
	public BaseObject getParentalData() {

		return new BaseNativeObject(this.change.getData());
	}

	@Override
	public String getParentGuid() {

		return this.change.getGuid();
	}

	@Override
	protected StorageImpl getPlugin() {

		return this.storage;
	}

	@Override
	public BaseSchedule getSchedule() {

		return new AbstractSchedule() {

			@Override
			public void commit() {

				ChangeForNewNested.this.changeSchedule = this;
			}
		};
	}

	@Override
	public StorageImpl getStorageImpl() {

		return this.storage;
	}

	@Override
	public BaseSync getSynchronization() {

		return new AbstractSync() {

			@Override
			public void commit() {

				ChangeForNewNested.this.changeSync = this;
			}
		};
	}

	@Override
	public String getVersionId() {

		return this.versionId;
	}

	@Override
	public void nestUnlink(final BaseEntry<?> entry, final boolean soft) {

		if (entry == null) {
			return;
		}
		if (entry.getClass() != EntryImpl.class || entry.getStorageImpl() != this.storage) {
			entry.createChange().unlink(soft);
			return;
		}
		if (this.children == null) {
			synchronized (this) {
				if (this.children == null) {
					this.children = new ArrayList<>();
				}
			}
		}
		this.children.add(new ChangeDoDelete((EntryImpl) entry, soft));
		return;
	}

	@Override
	public boolean realCommit(final Transaction transaction) throws Throwable {

		String typeName = Base.getString(this.data, "$type", null);
		if (typeName == null) {
			typeName = this.storage.getServer().getTypes().getTypeNameDefault();
		}
		final Type<?> type = this.storage.getServer().getTypes().getType(typeName);
		if (type != null) {
			type.onBeforeCreate(this, this.data);
		}
		if (this.changeSchedule != null) {
			final BaseSchedule schedule = this.storage.getScheduling().createChange(this.guid);
			this.changeSchedule.scheduleFill(schedule, true);
			schedule.commit();
		}
		if (this.changeSync != null) {
			final BaseSync sync = this.storage.getSynchronizer().createChange(this.guid);
			this.changeSync.synchronizeFill(sync);
			sync.commit();
		}
		String key = null;
		String title = "Document title!";
		long created = System.currentTimeMillis();
		int state = ModuleInterface.STATE_DRAFT;
		boolean folder = false;
		String owner = Context.getUserId(Exec.currentProcess());
		for (final Iterator<String> i = Base.keys(this.data); i.hasNext();) {
			final String currentKey = i.next();
			if (currentKey.length() > 0 && '$' == currentKey.charAt(0)) {
				if ("$key".equals(currentKey)) {
					key = this.data.baseGet(currentKey, BaseObject.UNDEFINED).baseToJavaString();
				} else //
				if ("$title".equals(currentKey)) {
					title = this.data.baseGet(currentKey, BaseObject.UNDEFINED).baseToJavaString();
				} else //
				if ("$created".equals(currentKey)) {
					created = this.data.baseGet(currentKey, BaseObject.UNDEFINED).baseToNumber().longValue();
				} else //
				if ("$state".equals(currentKey)) {
					state = this.data.baseGet(currentKey, BaseObject.UNDEFINED).baseToJavaInteger();
				} else //
				if ("$type".equals(currentKey)) {
					typeName = this.data.baseGet(currentKey, BaseObject.UNDEFINED).baseToJavaString();
				} else //
				if ("$folder".equals(currentKey)) {
					folder = this.data.baseGet(currentKey, BaseObject.UNDEFINED).baseToJavaBoolean();
				} else //
				if ("$owner".equals(currentKey)) {
					/** Owner could be explicitly set on newly created entries */
					owner = this.data.baseGet(currentKey, BaseObject.UNDEFINED).baseToJavaString();
				}
				i.remove();
			}
		}
		if (key == null) {
			key = this.createDefaultKey();
			this.data.baseDefine("$key", key);
		}
		final BaseObject data = this.data;
		final String objId;
		if (this.linkedWith == null) {
			objId = Engine.createGuid();
			final String versionId;
			if (this.getVersioning() && this.getVersionData() != null) {
				versionId = Engine.createGuid();
				this.versionId = versionId;
			} else {
				versionId = null;
			}
			transaction.create(
					this.local,
					this.parentGuid,
					this.guid,
					objId,
					key,
					folder,
					created,
					owner,
					state,
					title,
					typeName,
					data,
					versionId,
					this.getVersionComment(),
					this.getVersionData());
		} else {
			objId = this.linkedWith.getLinkedIdentity();
			transaction.link(this.local, this.parentGuid, this.guid, key, folder, this.linkedWith.getLinkedIdentity());
		}
		if (this.aliasAdd != null) {
			transaction.aliases(this.guid, this.aliasAdd, null);
			this.aliasAdd = null;
		}
		data.baseDefine("$key", key);
		data.baseDefine("$title", title);
		data.baseDefine("$state", state);
		data.baseDefine("$folder", folder);
		data.baseDefine("$type", typeName);
		data.baseDefine("$created", Base.forDateMillis(created));
		if (this.linkedIn != null) {
			for (final String current : this.linkedIn) {
				final int pos = current.indexOf('\n');
				if (pos == -1) {
					transaction.link(this.local, current, Engine.createGuid(), key, folder, objId);
				} else {
					transaction.link(this.local, current.substring(0, pos), Engine.createGuid(), current.substring(pos + 1), folder, objId);
				}
			}
			this.linkedIn = null;
		}
		if (this.children != null && !this.children.isEmpty()) {
			for (final ChangeNested child : this.children) {
				child.realCommit(transaction);
			}
		}
		return true;
	}

	@Override
	public final void segregate() {

		throw new UnsupportedOperationException("'segregate' is unsupported on non-commited entries!");
	}

	@Override
	public void setCreateLinkedIn(final BaseEntry<?> folder) {

		if (this.linkedIn == null) {
			this.linkedIn = Create.tempSet();
		}
		this.linkedIn.add(folder.getGuid());
	}

	@Override
	public void setCreateLinkedIn(final BaseEntry<?> folder, final String key) {

		if (this.linkedIn == null) {
			this.linkedIn = Create.tempSet();
		}
		this.linkedIn.add(folder.getGuid() + '\n' + key);
	}

	@Override
	public void setCreateLinkedWith(final BaseEntry<?> entry) {

		this.linkedWith = entry;
	}

	@Override
	public void setCreateLocal(final boolean local) {

		this.local = local;
	}

	@Override
	public void setParentGuid(final String parentGuid) {

		this.parentGuid = parentGuid;
	}

	@Override
	public String toString() {

		return "[object " + this.baseClass() + "(" + this.toStringDetails() + ")]";
	}

	private String toStringDetails() {

		final StringBuilder buffer = new StringBuilder();
		for (final Iterator<String> iterator = Base.keys(this.data); iterator.hasNext();) {
			final String key = iterator.next();
			if (key.length() > 0 && '$' == key.charAt(0)) {
				buffer.append(", ");
				buffer.append(key);
				buffer.append('=');
				buffer.append(this.data.baseGet(key, BaseObject.UNDEFINED));
			}
		}
		return "guid=" + this.guid + ", parentGuid=" + this.parentGuid + buffer;
	}

	@Override
	public final void unlink() {

		this.unlink(false);
	}

	@Override
	public final void unlink(final boolean soft) {

		throw new UnsupportedOperationException("'unlink' is unsupported on non-commited entries!");
	}
}
