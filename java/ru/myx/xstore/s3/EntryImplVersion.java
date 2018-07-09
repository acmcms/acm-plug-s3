/*
 * Created on 10.11.2005
 */
package ru.myx.xstore.s3;

import ru.myx.ae1.storage.BaseChange;
import ru.myx.xstore.s3.concept.LinkData;

final class EntryImplVersion extends EntryImpl {

	private final StorageLevel3 storage;

	private final EntryImpl entry;

	private final String versionId;

	EntryImplVersion(final StorageLevel3 storage, final EntryImpl entry, final LinkData link, final String versionId) {

		super(storage, link);
		this.storage = storage;
		this.entry = entry;
		this.versionId = versionId;
	}

	@Override
	public BaseChange createChange() {

		return new ChangeForVersion(this.storage, this, this.versionId, this.entry.getLink(), this.entry.getDataClean());
	}

	@Override
	public String toString() {

		return "[object " + this.baseClass() + "(" + this.toStringDetails() + ")]";
	}

	@Override
	protected String toStringDetails() {

		return "id=" + this.getGuid() + ", versionId=" + this.versionId + ", title=" + this.getTitle();
	}
}
