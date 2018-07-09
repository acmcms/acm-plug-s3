/*
 * Created on 10.11.2005
 */
package ru.myx.xstore.s3;

import ru.myx.ae1.storage.BaseChange;
import ru.myx.xstore.s3.concept.LinkData;

final class EntryImplHistory extends EntryImpl {

	private final StorageLevel3 storage;

	private final String historyId;

	EntryImplHistory(final StorageLevel3 storage, final LinkData link, final String historyId) {

		super(storage, link);
		this.storage = storage;
		this.historyId = historyId;
	}

	@Override
	public BaseChange createChange() {

		return new ChangeForHistory(this.storage, this, this.historyId);
	}

	@Override
	public String toString() {

		return "[object " + this.baseClass() + "(" + this.toStringDetails() + ")]";
	}

	@Override
	protected String toStringDetails() {

		return "id=" + this.getGuid() + ", historyId=" + this.historyId + ", title=" + this.getTitle();
	}
}
