/*
 * Created on 20.08.2004
 *
 * Window - Preferences - Java - Code Style - Code Templates
 */
package ru.myx.xstore.s3.jdbc;

import ru.myx.ae1.storage.BaseHistory;

/** @author myx
 *
 *         Window - Preferences - Java - Code Style - Code Templates */
final class HistoryJdbc implements BaseHistory {

	private final String guid;

	private final long date;

	private final String title;

	HistoryJdbc(final String guid, final long date, final String title) {

		this.guid = guid;
		this.date = date;
		this.title = title;
	}

	@Override
	public long getDate() {

		return this.date;
	}

	@Override
	public String getGuid() {

		return this.guid;
	}

	@Override
	public String getTitle() {

		return this.title;
	}
}
