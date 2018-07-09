/*
 * Created on 20.08.2004
 *
 * Window - Preferences - Java - Code Style - Code Templates
 */
package ru.myx.xstore.s3.jdbc;

import java.sql.Connection;

import ru.myx.ae1.storage.BaseRecycled;
import ru.myx.xstore.s3.concept.InvalidationEventType;

/** @author myx
 *
 *         Window - Preferences - Java - Code Style - Code Templates */
final class RecycledJdbc implements BaseRecycled {

	private final long date;

	private final String folder;

	private final String guid;

	private final String owner;

	private final ServerJdbc server;

	private final String title;

	RecycledJdbc(final ServerJdbc server, final String guid, final long date, final String title, final String folder, final String owner) {

		this.server = server;
		this.guid = guid;
		this.date = date;
		this.title = title;
		this.folder = folder;
		this.owner = owner;
	}

	@Override
	public boolean canClean() {

		return true;
	}

	@Override
	public boolean canMove() {

		return true;
	}

	@Override
	public boolean canRestore() {

		try {
			return this.server.getLink(this.folder) != null;
		} catch (final RuntimeException e) {
			throw e;
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void doClean() {

		try (final Connection conn = this.server.getStorage().nextConnection()) {
			try {
				conn.setAutoCommit(false);
				MatRecycled.clearRecycled(conn, this.guid);
				conn.commit();
			} catch (final Throwable t) {
				try {
					conn.rollback();
				} catch (final Throwable tt) {
					// ignore
				}
				throw t;
			}
		} catch (final Error e) {
			throw e;
		} catch (final RuntimeException e) {
			throw e;
		} catch (final Throwable t) {
			throw new RuntimeException(t);
		}
	}

	@Override
	public void doMove(final String parentGuid) {

		try (final Connection conn = this.server.getStorage().nextConnection()) {
			try {
				conn.setAutoCommit(false);
				MatRecycled.restoreRecycled(this.server, conn, this.guid, parentGuid);
				conn.commit();
				MatChange.serializeInvalidation(this.server.getStorage(), conn, InvalidationEventType.ULINK, this.guid);
				MatChange.serializeInvalidation(this.server.getStorage(), conn, InvalidationEventType.UTREE, parentGuid);
				conn.commit();
			} catch (final Throwable t) {
				try {
					conn.rollback();
				} catch (final Throwable tt) {
					// ignore
				}
				throw t;
			}
		} catch (final Error e) {
			throw e;
		} catch (final RuntimeException e) {
			throw e;
		} catch (final Throwable t) {
			throw new RuntimeException(t);
		}
	}

	@Override
	public void doRestore() {

		try (final Connection conn = this.server.getStorage().nextConnection()) {
			try {
				conn.setAutoCommit(false);
				MatRecycled.restoreRecycled(this.server, conn, this.guid);
				conn.commit();
				MatChange.serializeInvalidation(this.server.getStorage(), conn, InvalidationEventType.ULINK, this.guid);
				MatChange.serializeInvalidation(this.server.getStorage(), conn, InvalidationEventType.UTREE, this.folder);
				conn.commit();
			} catch (final Throwable t) {
				try {
					conn.rollback();
				} catch (final Throwable tt) {
					// ignore
				}
				throw t;
			}
		} catch (final Error e) {
			throw e;
		} catch (final RuntimeException e) {
			throw e;
		} catch (final Throwable t) {
			throw new RuntimeException(t);
		}
	}

	@Override
	public long getDate() {

		return this.date;
	}

	@Override
	public String getFolder() {

		return this.folder;
	}

	@Override
	public String getGuid() {

		return this.guid;
	}

	@Override
	public String getOwner() {

		return this.owner;
	}

	@Override
	public String getTitle() {

		return this.title;
	}
}
