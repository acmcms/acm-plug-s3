/*
 * Created on 25.08.2004
 *
 * Window - Preferences - Java - Code Style - Code Templates
 */
package ru.myx.xstore.s3.jdbc;

import ru.myx.ae1.storage.BaseVersion;

/** @author myx
 *
 *         Window - Preferences - Java - Code Style - Code Templates */
final class VersionJdbc implements BaseVersion {

	private final String vrId;

	private final long vrDate;

	private final String vrParentId;

	private final String vrComment;

	private final String vrTitle;

	private final String vrOwner;

	private final String vrType;

	VersionJdbc(final String vrId, final long vrDate, final String vrParentId, final String vrComment, final String vrTitle, final String vrOwner, final String vrType) {

		this.vrId = vrId;
		this.vrDate = vrDate;
		this.vrParentId = vrParentId;
		this.vrComment = vrComment;
		this.vrTitle = vrTitle;
		this.vrOwner = vrOwner;
		this.vrType = vrType;
	}

	@Override
	public String getComment() {

		return this.vrComment;
	}

	@Override
	public long getDate() {

		return this.vrDate;
	}

	@Override
	public String getGuid() {

		return this.vrId;
	}

	@Override
	public String getOwner() {

		return this.vrOwner;
	}

	@Override
	public String getParentGuid() {

		return this.vrParentId;
	}

	@Override
	public String getTitle() {

		return this.vrTitle;
	}

	@Override
	public String getTypeName() {

		return this.vrType;
	}
}
