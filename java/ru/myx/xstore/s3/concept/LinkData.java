/*
 * Created on 18.10.2004 Window - Preferences - Java - Code Style - Code
 * Templates
 */
package ru.myx.xstore.s3.concept;

import java.sql.Connection;
import java.util.ConcurrentModificationException;

import ru.myx.ae1.types.Type;
import ru.myx.ae3.Engine;
import ru.myx.ae3.act.Act;
import ru.myx.ae3.base.BaseObject;
import ru.myx.ae3.base.BaseSealed;
import ru.myx.ae3.common.Value;
import ru.myx.ae3.common.ValueSimple;
import ru.myx.ae3.help.Convert;
import ru.myx.ae3.report.Report;
import ru.myx.jdbc.queueing.DatabaseWaitTimeoutException;
import ru.myx.xstore.s3.StorageLevel3;

/** @author myx */
public final class LinkData implements Comparable<LinkData> {
	
	private static final Value<BaseObject> EMPTY_MAP_REFERENCE = new ValueSimple<>(BaseObject.UNDEFINED);
	
	private static final String getLetter(final String letter) {
		
		for (int i = 0; i < letter.length(); ++i) {
			final char c = letter.charAt(i);
			if (Character.isLetterOrDigit(c)) {
				return String.valueOf(c);
			}
		}
		return null;
	}

	/**
	 *
	 */
	public boolean invalid = false;

	/**
	 *
	 */
	public final String lnkId;

	/**
	 *
	 */
	public int lnkLuid;

	/**
	 *
	 */
	public String lnkCntId;

	/**
	 *
	 */
	public String lnkName;

	/**
	 *
	 */
	public boolean lnkFolder;

	/**
	 *
	 */
	public String objId;

	/**
	 *
	 */
	public String vrId;

	/**
	 *
	 */
	public String letter;

	/**
	 *
	 */
	public String objTitle;

	/**
	 *
	 */
	public long objCreated;

	/**
	 *
	 */
	public long objModified;

	/**
	 *
	 */
	public String objOwner;

	/**
	 *
	 */
	public String objType;

	/**
	 *
	 */
	public int objState;

	/**
	 *
	 */
	public boolean listable;

	/**
	 *
	 */
	public boolean searchable;

	/**
	 *
	 */
	public String extLink;

	private boolean loadDone = false;

	private LinkData loadResult = null;

	private BaseObject dataMap = null;

	private Value<BaseObject> dataReference = null;
	
	/** @param lnkId
	 */
	public LinkData(final String lnkId) {
		
		this.lnkId = lnkId;
	}
	
	/** @param lnkId
	 * @param lnkLuid
	 * @param lnkCntId
	 * @param lnkName
	 * @param lnkFolder
	 * @param objId
	 * @param vrId
	 * @param objTitleLetter
	 * @param objCreated
	 * @param objModified
	 * @param objOwner
	 * @param objType
	 * @param objState
	 * @param listable
	 * @param searchable
	 * @param extLink
	 */
	public LinkData(
			final String lnkId,
			final int lnkLuid,
			final String lnkCntId,
			final String lnkName,
			final boolean lnkFolder,
			final String objId,
			final String vrId,
			final char objTitleLetter,
			final long objCreated,
			final long objModified,
			final String objOwner,
			final String objType,
			final int objState,
			final boolean listable,
			final boolean searchable,
			final String extLink) {
		
		this.lnkId = lnkId;
		this.lnkLuid = lnkLuid;
		this.lnkCntId = lnkCntId;
		this.lnkName = lnkName;
		this.lnkFolder = lnkFolder;
		this.objId = objId;
		this.vrId = vrId;
		this.letter = objTitleLetter == 0
			? null
			: String.valueOf(objTitleLetter);
		this.objTitle = null;
		this.objCreated = objCreated;
		this.objModified = objModified;
		this.objOwner = objOwner;
		this.objType = objType;
		this.objState = objState;
		this.listable = listable;
		this.searchable = searchable;
		this.extLink = extLink;
		this.loadResult = this;
		this.loadDone = true;
		if (lnkLuid == -1) {
			this.invalid = true;
		}
	}
	
	/** @param lnkId
	 * @param lnkLuid
	 * @param lnkCntId
	 * @param lnkName
	 * @param lnkFolder
	 * @param objId
	 * @param vrId
	 * @param objTitle
	 * @param objCreated
	 * @param objModified
	 * @param objOwner
	 * @param objType
	 * @param objState
	 * @param listable
	 * @param searchable
	 * @param extLink
	 */
	public LinkData(
			final String lnkId,
			final int lnkLuid,
			final String lnkCntId,
			final String lnkName,
			final boolean lnkFolder,
			final String objId,
			final String vrId,
			final String objTitle,
			final long objCreated,
			final long objModified,
			final String objOwner,
			final String objType,
			final int objState,
			final boolean listable,
			final boolean searchable,
			final String extLink) {
		
		this.lnkId = lnkId;
		this.lnkLuid = lnkLuid;
		this.lnkCntId = lnkCntId;
		this.lnkName = lnkName;
		this.lnkFolder = lnkFolder;
		this.objId = objId;
		this.vrId = vrId;
		this.letter = LinkData.getLetter(objTitle);
		this.objTitle = objTitle;
		this.objCreated = objCreated;
		this.objModified = objModified;
		this.objOwner = objOwner;
		this.objType = objType;
		this.objState = objState;
		this.listable = listable;
		this.searchable = searchable;
		this.extLink = extLink;
		this.loadResult = this;
		this.loadDone = true;
		if (lnkLuid == -1) {
			this.invalid = true;
		}
	}
	
	@Override
	public final int compareTo(final LinkData arg0) {
		
		return this.lnkId.compareTo(arg0.lnkId);
	}
	
	/** @param storage
	 * @param typeNameOverride
	 * @return map */
	public final BaseObject getData(final StorageLevel3 storage, final String typeNameOverride) {
		
		if (this.dataMap == null) {
			synchronized (this) {
				if (this.dataMap == null) {
					final Type<?> type = storage.getServer().getTypes().getType(
							typeNameOverride == null
								? this.objType
								: typeNameOverride);
					if (type == null) {
						this.dataMap = new BaseSealed(this.getDataReal(storage, null));
					} else {
						this.dataMap = Type.getDataAccessMapDefaultImpl(
								type,
								typeNameOverride == null
									? new FieldsReadable(storage, this)
									: new FieldsReadableTypeOverride(storage, this, typeNameOverride));
					}
				}
			}
		}
		return this.dataMap;
	}
	
	/** @param storage
	 * @param attachment
	 * @return data */
	public final BaseObject getDataReal(final StorageLevel3 storage, final Connection attachment) {
		
		BaseObject data;
		try {
			data = this.dataReference == null
				? null
				: this.dataReference.baseValue();
		} catch (final ConcurrentModificationException e) {
			assert Report.MODE_DEBUG && Report.exception("S3-LINK-DATA", "descr=" + this.toString(), "Cache entry looks to be deleted concurrently, trying to recover", e);
			data = null;
		}
		if (data == null) {
			synchronized (this) {
				try {
					data = this.dataReference == null
						? null
						: this.dataReference.baseValue();
				} catch (final ConcurrentModificationException e) {
					assert Report.MODE_DEBUG && Report.exception("S3-LINK-DATA", "descr=" + this.toString(), "Cache entry looks to be deleted concurrently, trying to recover", e);
					data = null;
				}
				if (data == null) {
					final Object o;
					try {
						if (this.extLink != null && this.extLink.length() == 1 && this.extLink.charAt(0) == '*') {
							o = null;
						} else {
							o = storage.getServerInterface().getExternalizerObject().getExternal(attachment, this.extLink);
						}
					} catch (final RuntimeException e) {
						throw e;
					} catch (final Exception e) {
						throw new RuntimeException(e);
					}
					if (o == null) {
						this.dataReference = LinkData.EMPTY_MAP_REFERENCE;
						return BaseObject.UNDEFINED;
					}
					assert o instanceof Value<?> : "Expected to be an instance of " + Value.class.getName();
					final Value<BaseObject> holder = Convert.Any.toAny(o);
					final BaseObject map = holder.baseValue();
					if (map == null) {
						this.dataReference = LinkData.EMPTY_MAP_REFERENCE;
						return BaseObject.UNDEFINED;
					}
					this.dataReference = holder;
					return map;
				}
			}
		}
		return data;
	}
	
	/** @return linkData */
	public final LinkData getLoadValue() {
		
		if (this.loadDone) {
			return this.loadResult;
		}
		synchronized (this) {
			if (this.loadDone) {
				return this.loadResult;
			}
			if (this.loadResult == null) {
				try {
					/** I AM NOT PARANOID.
					 *
					 *
					 * Taken from Javadoc for 'wait' method:
					 *
					 * A thread can also wake up without being notified, interrupted, or timing out,
					 * a so-called spurious wakeup. While this will rarely occur in practice,
					 * applications must guard against it by testing for the condition that should
					 * have caused the thread to be awakened, and continuing to wait if the
					 * condition is not satisfied. In other words, waits should always occur in
					 * loops, like this one:
					 *
					 * synchronized (obj) { while (<condition does not hold>) obj.wait(timeout); ...
					 * // Perform action appropriate to condition }
					 *
					 * (For more information on this topic, see Section 3.2.3 in Doug Lea's
					 * "Concurrent Programming in Java (Second Edition)" (Addison-Wesley, 2000), or
					 * Item 50 in Joshua Bloch's "Effective Java Programming Language Guide"
					 * (Addison-Wesley, 2001). */
					for (//
							long left = 30000L, expires = Engine.fastTime() + left; //
							left > 0; //
							left = expires - Engine.fastTime()) {
						//
						this.wait(left);
						if (this.loadDone) {
							// if (this.error != null) {
							// throw this.error;
							// }
							return this.loadResult;
						}
						if (this.loadResult != null) {
							break;
						}
					}
				} catch (final InterruptedException e) {
					return null;
				}
			}
		}
		if (this.loadResult == null) {
			/** TODO: change null to something meaningful, please */
			throw new DatabaseWaitTimeoutException("Link access timeout!", null);
		}
		if (this.loadResult == this) {
			return this.loadResult;
		}
		final LinkData result = this.loadResult.getLoadValue();
		this.loadResult = result;
		this.loadDone = true;
		return result;
	}
	
	/** @return result */
	public final boolean getVersioning() {
		
		return !"*".equals(this.vrId);
	}
	
	/**
	 */
	public final void invalidateThis() {
		
		this.dataReference = null;
		this.dataMap = null;
	}
	
	/** @param data
	 */
	public final void setDataReal(final BaseObject data) {
		
		this.dataReference = new ValueSimple<>(data);
		this.dataMap = null;
	}
	
	/** @param link
	 */
	public final void setDuplicateOf(final LinkData link) {
		
		assert link != this : "Duplicate of itself?";
		this.loadResult = link;
		Act.launchNotifyAll(this);
	}
	
	/**
	 *
	 */
	public void setLoadNotFound() {
		
		// this.loadResult = null;
		this.loadDone = true;
		Act.launchNotifyAll(this);
	}
	
	/** @param lnkLuid
	 * @param lnkCntId
	 * @param lnkName
	 * @param lnkFolder
	 * @param objId
	 * @param vrId
	 * @param objTitle
	 * @param objCreated
	 * @param objModified
	 * @param objOwner
	 * @param objType
	 * @param objState
	 * @param listable
	 * @param searchable
	 * @param extLink
	 */
	public final void setLoadResult(final int lnkLuid,
			final String lnkCntId,
			final String lnkName,
			final boolean lnkFolder,
			final String objId,
			final String vrId,
			final String objTitle,
			final long objCreated,
			final long objModified,
			final String objOwner,
			final String objType,
			final int objState,
			final boolean listable,
			final boolean searchable,
			final String extLink) {
		
		this.lnkLuid = lnkLuid;
		this.lnkCntId = lnkCntId;
		this.lnkName = lnkName;
		this.lnkFolder = lnkFolder;
		this.objId = objId;
		this.vrId = vrId;
		this.letter = LinkData.getLetter(objTitle);
		this.objTitle = objTitle;
		this.objCreated = objCreated;
		this.objModified = objModified;
		this.objOwner = objOwner;
		this.objType = objType;
		this.objState = objState;
		this.listable = listable;
		this.searchable = searchable;
		this.extLink = extLink;
		this.loadResult = this;
		this.loadDone = true;
		Act.launchNotifyAll(this);
	}
	
	@Override
	public String toString() {
		
		return "LinkData{folder=" + this.lnkCntId + ", name=" + this.lnkName + ", type=" + this.objType + "}";
	}
}
