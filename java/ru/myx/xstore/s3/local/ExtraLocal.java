/*
 * Created on 12.09.2005
 */
package ru.myx.xstore.s3.local;

import java.util.ConcurrentModificationException;

import ru.myx.ae3.extra.AbstractExtra;
import ru.myx.ae3.extra.External;
import ru.myx.ae3.report.Report;

final class ExtraLocal extends AbstractExtra {

	private static final External NULL_OBJECT = new ExtraLocal(null, null, -1);

	private External extra = null;

	private final Leaf leaf;

	private final int folderIndex;

	ExtraLocal(final String recId, final Leaf leaf, final int folderIndex) {

		super(recId);
		this.leaf = leaf;
		this.folderIndex = folderIndex;
	}

	private final Object baseValueRetry() {

		Thread.yield();
		this.extra = null;
		Thread.yield();
		final External extra = this.getExtra();
		try {
			return extra != null
				? extra.baseValue()
				: null;
		} catch (final ConcurrentModificationException ee) {
			Report.exception(//
					"EXTRA-LOCAL",
					"id=" + this.recId,
					"Cache entry looks to be deleted concurrently, while trying to recover",
					new Error(ee)//
			);
			throw ee;
		}
	}

	private final External getExtra() {

		if (this.extra == null) {
			synchronized (this) {
				if (this.extra == null) {
					final External extra = this.leaf.getExternal(this.folderIndex, null, this.recId);
					this.extra = extra == null
						? ExtraLocal.NULL_OBJECT
						: extra;
					return extra;
				}
			}
		}
		return this.extra == ExtraLocal.NULL_OBJECT
			? null
			: this.extra;
	}

	@Override
	public final Object baseValue() {

		try {
			final External extra = this.getExtra();
			return extra != null
				? extra.baseValue()
				: null;
		} catch (final ConcurrentModificationException e) {
			return this.baseValueRetry();
		}
	}

	@Override
	public final long getRecordDate() {

		return this.getExtra().getRecordDate();
	}

	@Override
	public final Object getRecordIssuer() {

		return this.leaf.getIssuer();
	}

	@Override
	public final Object toBinary() {

		final External extra = this.getExtra();
		return extra == null
			? null
			: extra.toBinary();
	}

	@Override
	public final String toString() {

		return String.valueOf(this.getExtra());
	}
}
