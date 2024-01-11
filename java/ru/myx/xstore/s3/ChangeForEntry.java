/*
 * Created on 30.06.2004
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
import ru.myx.ae1.storage.BaseHistory;
import ru.myx.ae1.storage.BaseSchedule;
import ru.myx.ae1.storage.BaseSync;
import ru.myx.ae1.storage.BaseVersion;
import ru.myx.ae1.storage.EntryCommiterTask;
import ru.myx.ae1.storage.StorageImpl;
import ru.myx.ae1.types.Type;
import ru.myx.ae3.Engine;
import ru.myx.ae3.act.Context;
import ru.myx.ae3.base.Base;
import ru.myx.ae3.base.BaseNativeObject;
import ru.myx.ae3.base.BaseObject;
import ru.myx.ae3.exec.Exec;
import ru.myx.ae3.help.Create;
import ru.myx.xstore.s3.concept.LinkData;
import ru.myx.xstore.s3.concept.Transaction;

/** @author myx
 *
 *         To change the template for this generated type comment go to
 *         Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments */
class ChangeForEntry extends ChangeAbstract {
	
	private Set<String> aliasAdd = null;
	
	private Set<String> aliasRemove = null;
	
	BaseSchedule changeSchedule = null;
	
	BaseSync changeSync = null;
	
	private List<ChangeNested> children = null;
	
	private boolean commitActive = false;
	
	private boolean commitLogged = false;
	
	private boolean commitUseVersionning = false;
	
	private final BaseObject data;
	
	private final EntryImpl entry;
	
	private long initialCreated;
	
	private boolean initialFolder;
	
	private String initialKey;
	
	private final String initialParentGuid;
	
	private int initialState;
	
	private String initialTitle;
	
	private String initialTypeName;
	
	private final String initialVersionId;
	
	private final boolean initialVersioning;
	
	private final BaseObject original;
	
	private String parentGuid;
	
	private boolean segregate = false;
	
	private final StorageLevel3 storage;
	
	private String versionId;
	
	private Set<String> linkedIn = null;
	
	private boolean local = false;
	
	private boolean touch = false;
	
	private boolean commited = false;
	
	ChangeForEntry(final StorageLevel3 storage, final EntryImpl entry) {
		
		this.storage = storage;
		this.entry = entry;
		this.initialParentGuid = entry.getParentGuid();
		this.parentGuid = this.initialParentGuid;
		this.initialKey = entry.getKey();
		this.initialState = entry.getState();
		this.initialFolder = entry.isFolder();
		this.initialVersionId = entry.getVersionId();
		this.initialVersioning = entry.getVersioning();
		this.setVersioning(this.initialVersioning);
		this.initialTitle = entry.getTitle();
		this.initialCreated = entry.getCreated();
		this.initialTypeName = entry.getTypeName();
		this.versionId = this.initialVersionId;
		this.original = new BaseNativeObject();
		this.original.baseDefineImportAllEnumerable(entry.getDataClean());
		this.data = new BaseNativeObject();
		this.data.baseDefineImportAllEnumerable(this.original);
		this.data.baseDefine("$guid", this.entry.getGuid());
		this.data.baseDefine("$key", this.initialKey);
		this.data.baseDefine("$title", this.initialTitle);
		this.data.baseDefine("$state", this.initialState);
		this.data.baseDefine("$folder", this.initialFolder);
		this.data.baseDefine("$type", this.initialTypeName);
		this.data.baseDefine("$created", Base.forDateMillis(this.initialCreated));
	}
	
	@Override
	public void aliasAdd(final String alias) {
		
		if (this.aliasAdd == null) {
			this.aliasAdd = Create.tempSet();
		}
		this.aliasAdd.add(alias);
		if (this.aliasRemove != null) {
			this.aliasRemove.remove(alias);
		}
	}
	
	@Override
	public void aliasRemove(final String alias) {
		
		if (this.aliasRemove == null) {
			this.aliasRemove = Create.tempSet();
		}
		this.aliasRemove.add(alias);
		if (this.aliasAdd != null) {
			this.aliasAdd.remove(alias);
		}
	}
	
	@Override
	public void commit() {
		
		assert this.commited == false && (this.commited = true) : "Already commited! (commit, delete, resync, unlink)";
		final Transaction transaction = this.storage.getServerNetwork().createTransaction();
		try {
			final boolean doClearVersions = this.initialVersioning && !this.getVersioning();
			final boolean doStartVersions = this.getVersioning() && !this.initialVersioning;
			final boolean doRecordHistory = this.commitLogged;
			final boolean doDeriveVersion = this.getVersioning() && this.commitUseVersionning;
			final boolean doUpdateObject = this.commitActive || !this.commitUseVersionning || !this.getVersioning();
			String typeName = Base.getString(this.data, "$type", null);
			if (typeName == null) {
				typeName = this.storage.getServer().getTypes().getTypeNameDefault();
			}
			final Type<?> type = this.storage.getServer().getTypes().getType(typeName);
			if (type != null && doUpdateObject) {
				type.onBeforeModify(this.entry, this, this.data);
			}
			if (this.changeSchedule != null) {
				final BaseSchedule schedule = this.entry.getSchedule();
				this.changeSchedule.scheduleFill(schedule, true);
				schedule.commit();
			}
			if (this.changeSync != null) {
				final BaseSync sync = this.entry.getSynchronization();
				this.changeSync.synchronizeFill(sync);
				sync.commit();
			}
			String key = this.initialKey;
			String title = this.initialTitle;
			long created = this.initialCreated;
			int state = this.initialState;
			boolean folder = this.initialFolder;
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
					if ("$guid".equals(currentKey)) {
						if (!this.entry.getGuid().equals(this.data.baseGet(currentKey, BaseObject.UNDEFINED).baseToJavaString())) {
							throw new UnsupportedOperationException("Can't change $guid field of an existing object!");
						}
					}
					i.remove();
				}
			}
			if (key == null) {
				key = this.createDefaultKey();
				this.data.baseDefine("$key", key);
			}
			if (this.initialParentGuid != this.parentGuid && !this.initialParentGuid.equals(this.parentGuid)) {
				transaction.move(this.entry.getLink(), this.parentGuid, key);
			} else //
			if (key != this.initialKey && !this.initialKey.equals(key)) {
				transaction.rename(this.entry.getLink(), key);
			}
			final String linkedIdentity;
			if (this.segregate) {
				linkedIdentity = Engine.createGuid();
				transaction.segregate(this.getGuid(), this.entry.getLinkedIdentity(), linkedIdentity);
			} else {
				linkedIdentity = this.entry.getLinkedIdentity();
			}
			final BaseObject data = this.data;
			final BaseObject removed = ChangeAbstract.changeGetRemoved(this.original, data);
			final BaseObject added = ChangeAbstract.changeGetAdded(this.original, data);
			final boolean objectModified;
			final String originalVersionId;
			if (doClearVersions) {
				transaction.versionClearAll(this.entry.getLink().objId);
				originalVersionId = "*";
			} else {
				if (doStartVersions) {
					originalVersionId = Engine.createGuid();
					transaction.versionStart(originalVersionId, "*", linkedIdentity, this.initialTitle, this.initialTypeName, this.entry.getOwner(), this.entry.getDataClean());
				} else {
					originalVersionId = this.initialVersionId;
				}
			}
			final boolean bothEmpty = !removed.baseHasKeysOwn() && !added.baseHasKeysOwn();
			final String targetVersionId;
			if (doDeriveVersion) {
				if (this.getVersioning() && !((this.initialTitle == title || this.initialTitle.equals(title))
						&& (this.initialTypeName == typeName || this.initialTypeName.equals(typeName)) && bothEmpty)) {
					targetVersionId = Engine.createGuid();
					this.versionId = targetVersionId;
					transaction.versionCreate(
							targetVersionId,
							originalVersionId,
							this.getVersionComment(),
							linkedIdentity,
							title,
							typeName,
							Context.getUserId(Exec.currentProcess()),
							this.getVersionData());
				} else {
					targetVersionId = originalVersionId;
				}
			} else {
				targetVersionId = originalVersionId;
			}
			if (doRecordHistory) {
				if (bothEmpty) {
					if ((this.initialTitle == title || this.initialTitle.equals(title)) && this.initialCreated == created
							&& (this.initialTypeName == typeName || this.initialTypeName.equals(typeName)) && this.initialState == state && this.initialFolder == folder) {
						objectModified = false;
					} else {
						if (this.commitLogged && !((this.initialTitle == title || this.initialTitle.equals(title)) && this.initialCreated == created
								&& (this.initialTypeName == typeName || this.initialTypeName.equals(typeName)) && this.initialState == state)) {
							transaction.record(linkedIdentity);
							objectModified = true;
						} else {
							objectModified = false;
						}
					}
				} else {
					if (this.commitLogged) {
						transaction.record(linkedIdentity);
					}
					objectModified = true;
				}
			} else {
				objectModified = false;
			}
			if (doUpdateObject) {
				if (bothEmpty) {
					if ((this.initialTitle == title || this.initialTitle.equals(title)) && this.initialCreated == created
							&& (this.initialTypeName == typeName || this.initialTypeName.equals(typeName)) && this.initialState == state && this.initialFolder == folder) {
						if (this.touch) {
							transaction.update(this.entry.getLink(), linkedIdentity);
						}
					} else {
						transaction.update(this.entry.getLink(), linkedIdentity, targetVersionId, folder, created, state, title, typeName, this.commitLogged);
					}
				} else {
					transaction.update(this.entry.getLink(), linkedIdentity, targetVersionId, folder, created, state, title, typeName, this.commitLogged, removed, added);
					
				}
			} else //
			if (doStartVersions) {
				transaction.update(
						this.entry.getLink(),
						linkedIdentity,
						targetVersionId,
						this.initialFolder,
						this.initialCreated,
						this.initialState,
						this.initialTitle,
						this.initialTypeName,
						this.commitLogged);
			}
			if (this.aliasAdd != null || this.aliasRemove != null) {
				transaction.aliases(this.entry.getGuid(), this.aliasAdd, this.aliasRemove);
				this.aliasAdd = null;
				this.aliasRemove = null;
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
						transaction.link(this.local, current, Engine.createGuid(), key, folder, this.getLinkedIdentity());
					} else {
						transaction.link(this.local, current.substring(0, pos), Engine.createGuid(), current.substring(pos + 1), folder, this.getLinkedIdentity());
					}
				}
				this.linkedIn = null;
			}
			if (this.children != null && !this.children.isEmpty()) {
				for (final ChangeNested child : this.children) {
					child.realCommit(transaction);
				}
			}
			transaction.commit();
			this.changeSchedule = null;
			this.changeSync = null;
			if (this.children != null) {
				this.children.clear();
			}
			if (objectModified) {
				this.commitLogged = false;
			}
			final BaseEntry<?> container = this.entry.getParent();
			if (container != null) {
				if (container.getType().getTypeBehaviorAutoRecalculate()) {
					EntryCommiterTask.touch(container);
				}
			}
			this.initialKey = key;
			this.initialTitle = title;
			this.initialState = state;
			this.initialFolder = folder;
			this.initialTypeName = typeName;
			this.initialCreated = created;
		} catch (final Throwable t) {
			transaction.rollback();
			throw new RuntimeException("Transaction cancelled", t);
		}
	}
	
	@Override
	public BaseChange createChange(final BaseEntry<?> entry) {
		
		if (entry == null) {
			return null;
		}
		if (entry.getClass() != EntryImpl.class || entry.getStorageImpl() != this.entry.getStorageImpl()) {
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
		
		assert this.commited == false && (this.commited = true) : "Already commited! (commit, delete, resync, unlink)";
		this.entry.getType().onBeforeDelete(this.entry);
		final Transaction transaction = this.storage.getServerNetwork().createTransaction();
		try {
			transaction.delete(this.entry.getLink(), soft);
			transaction.commit();
		} catch (final Error e) {
			transaction.rollback();
			throw e;
		} catch (final RuntimeException e) {
			transaction.rollback();
			throw e;
		} catch (final Throwable t) {
			transaction.rollback();
			throw new RuntimeException(t);
		}
	}
	
	@Override
	public BaseObject getData() {
		
		return this.data;
	}
	
	@Override
	public String getGuid() {
		
		return this.entry.getGuid();
	}
	
	@Override
	public BaseHistory[] getHistory() {
		
		return this.entry.getHistory();
	}
	
	@Override
	public BaseChange getHistorySnapshot(final String historyId) {
		
		final BaseEntry<?> entry = this.entry.getHistorySnapshot(historyId);
		if (entry == null) {
			return null;
		}
		final BaseChange change = entry.createChange();
		if (change == null) {
			return null;
		}
		if (this.commitLogged) {
			change.setCommitLogged();
		}
		return change;
	}
	
	@Override
	public String getLinkedIdentity() {
		
		return this.entry.getLinkedIdentity();
	}
	
	@Override
	public final String getLocationControl() {
		
		final StorageImpl parent = this.getStorageImpl();
		if (this.getGuid().equals(parent.getStorage().getRootIdentifier())) {
			return parent.getLocationControl();
		}
		final BaseEntry<?> entry = parent.getStorage().getByGuid(this.getParentGuid());
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
	public BaseObject getParentalData() {
		
		final BaseEntry<?> parent = this.entry.getParent();
		return parent == null
			? null
			: parent.getData();
	}
	
	@Override
	public String getParentGuid() {
		
		return this.entry.getParentGuid();
	}
	
	@Override
	protected StorageImpl getPlugin() {
		
		return this.entry.getStorageImpl();
	}
	
	@Override
	public BaseSchedule getSchedule() {
		
		return new AbstractSchedule(false, this.entry.getSchedule()) {
			
			@Override
			public void commit() {
				
				ChangeForEntry.this.changeSchedule = this;
			}
		};
	}
	
	@Override
	public StorageImpl getStorageImpl() {
		
		return this.entry.getStorageImpl();
	}
	
	@Override
	public BaseSync getSynchronization() {
		
		return new AbstractSync(this.storage.getSynchronizer().createChange(this.getGuid())) {
			
			@Override
			public void commit() {
				
				ChangeForEntry.this.changeSync = this;
			}
		};
	}
	
	@Override
	public BaseChange getVersion(final String versionId) {
		
		final BaseEntry<?> entry = this.entry.getVersion(versionId);
		if (entry == null) {
			return null;
		}
		final BaseChange change = entry.createChange();
		if (change == null) {
			return null;
		}
		if (this.commitLogged) {
			change.setCommitLogged();
		}
		return change;
	}
	
	@Override
	public String getVersionId() {
		
		return this.versionId;
	}
	
	@Override
	public BaseVersion[] getVersions() {
		
		return this.entry.getVersions();
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
	public final void resync() {
		
		assert this.commited == false && (this.commited = true) : "Already commited! (commit, delete, resync, unlink)";
		final Transaction transaction = this.storage.getServerNetwork().createTransaction();
		try {
			transaction.resync(this.getGuid());
			transaction.commit();
		} catch (final Error e) {
			transaction.rollback();
			throw e;
		} catch (final RuntimeException e) {
			transaction.rollback();
			throw e;
		} catch (final Throwable t) {
			transaction.rollback();
			throw new RuntimeException(t);
		}
	}
	
	@Override
	public final void segregate() {
		
		this.segregate = true;
	}
	
	@Override
	public void setCommitActive() {
		
		this.commitActive = true;
	}
	
	@Override
	public void setCommitLogged() {
		
		this.commitLogged = true;
		this.commitUseVersionning = true;
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
		
		throw new UnsupportedOperationException("Only new uncommited objects can be linked!");
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
		return "parentGuid=" + this.parentGuid + buffer;
	}
	
	@Override
	public void touch() {
		
		this.touch = true;
	}
	
	@Override
	public final void unlink() {
		
		this.unlink(false);
	}
	
	@Override
	public final void unlink(final boolean soft) {
		
		assert this.commited == false && (this.commited = true) : "Already commited! (commit, delete, resync, unlink)";
		this.entry.getType().onBeforeDelete(this.entry);
		final Transaction transaction = this.storage.getServerNetwork().createTransaction();
		try {
			final LinkData link = this.entry.getLink();
			transaction.unlink(link, soft);
			transaction.commit();
		} catch (final Error e) {
			transaction.rollback();
			throw e;
		} catch (final RuntimeException e) {
			transaction.rollback();
			throw e;
		} catch (final Throwable t) {
			transaction.rollback();
			throw new RuntimeException(t);
		}
	}
}
