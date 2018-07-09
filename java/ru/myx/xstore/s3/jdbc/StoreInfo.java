/*
 * Created on 29.07.2004
 *
 * Window - Preferences - Java - Code Style - Code Templates
 */
package ru.myx.xstore.s3.jdbc;

import java.sql.Connection;

/** @author myx */
final class StoreInfo {

	private final Connection connection;

	private final String objId;

	boolean preferInsert;

	StoreInfo(final Connection connection, final String objId, final boolean preferInsert) {

		this.connection = connection;
		this.objId = objId;
		this.preferInsert = preferInsert;
	}

	final Connection getConnection() {

		return this.connection;
	}

	final String getObjId() {

		return this.objId;
	}
}
