/*
 * Created on 20.09.2004
 *
 * Window - Preferences - Java - Code Style - Code Templates
 */
package ru.myx.xstore.s3;

import java.util.Iterator;

import ru.myx.ae1.storage.AbstractChange;
import ru.myx.ae1.storage.StorageImpl;
import ru.myx.ae3.base.Base;
import ru.myx.ae3.base.BaseNativeObject;
import ru.myx.ae3.base.BaseObject;

/** @author myx
 *
 *         Window - Preferences - Java - Code Style - Code Templates */
abstract class ChangeAbstract extends AbstractChange {

	protected static final BaseObject changeGetAdded(final BaseObject original, final BaseObject current) {

		final BaseObject result = new BaseNativeObject(null);
		for (final Iterator<String> iterator = Base.keys(current); iterator.hasNext();) {
			final String key = iterator.next();
			final BaseObject value = current.baseGet(key, BaseObject.UNDEFINED);
			assert value != null : "NULL java value";
			final BaseObject previous = original.baseGet(key, BaseObject.UNDEFINED);
			assert previous != null : "NULL java value";
			if (value != previous && !(value.equals(previous) || previous.equals(value))) {
				result.baseDefine(key, value);
			}
		}
		return result;
	}

	protected static final BaseObject changeGetRemoved(final BaseObject original, final BaseObject current) {

		final BaseObject result = new BaseNativeObject(null);
		for (final Iterator<String> iterator = Base.keys(original); iterator.hasNext();) {
			final String key = iterator.next();
			final BaseObject value = original.baseGet(key, BaseObject.UNDEFINED);
			assert value != null : "NULL java value";
			final BaseObject previous = current.baseGet(key, BaseObject.UNDEFINED);
			assert previous != null : "NULL java value";
			if (value != previous && !(value.equals(previous) || previous.equals(value))) {
				result.baseDefine(key, value);
			}
		}
		return result;
	}

	private String versionComment = null;

	private BaseObject versionData = null;

	private boolean versioning;

	protected abstract StorageImpl getPlugin();

	final String getVersionComment() {

		return this.versionComment;
	}

	@Override
	public final BaseObject getVersionData() {

		return this.versionData;
	}

	@Override
	public final boolean getVersioning() {

		return this.versioning;
	}

	@Override
	public final boolean isMultivariant() {

		return false;
	}

	@Override
	public final void setVersionComment(final String versionComment) {

		this.versionComment = versionComment;
	}

	@Override
	public final void setVersionData(final BaseObject versionData) {

		this.versionData = versionData;
	}

	@Override
	public final void setVersioning(final boolean versioning) {

		this.versioning = versioning;
	}
}
