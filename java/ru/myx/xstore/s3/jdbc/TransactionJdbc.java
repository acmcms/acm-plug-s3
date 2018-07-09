package ru.myx.xstore.s3.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import ru.myx.ae3.act.Context;
import ru.myx.ae3.base.BaseObject;
import ru.myx.ae3.exec.Exec;
import ru.myx.ae3.report.Report;
import ru.myx.xstore.s3.concept.InvalidationCollector;
import ru.myx.xstore.s3.concept.InvalidationEventType;
import ru.myx.xstore.s3.concept.LinkData;
import ru.myx.xstore.s3.concept.Transaction;

final class TransactionJdbc implements Transaction {

	private final long started;

	private boolean closed = false;

	private final Connection conn;

	private InvalidationCollector invalidations = null;

	private final Object issuer;

	private final ServerJdbc server;

	TransactionJdbc(final ServerJdbc server, final Connection conn, final Object issuer) {

		this.server = server;
		this.conn = conn;
		this.issuer = issuer;
		this.started = System.currentTimeMillis();
	}

	@Override
	public final void aliases(final String lnkId, final Set<String> added, final Set<String> removed) throws Throwable {

		if (removed != null) {
			for (final String alias : removed) {
				this.invalidateUpdateIden(alias);
			}
		}
		if (added != null) {
			for (final String alias : added) {
				this.invalidateUpdateIden(alias);
			}
		}
		MatAlias.update(this.conn, lnkId, added, removed);
	}

	@Override
	public final void commit() {

		if (this.closed) {
			return;
		}
		try {
			this.conn.commit();
			this.server.stsTransactionsCommited++;
		} catch (final SQLException e) {
			throw new RuntimeException(e);
		} finally {
			try {
				if (this.invalidations != null) {
					MatChange.serializeInvalidations(this.server.getStorage(), this.conn, this.invalidations);
					this.invalidations = null;
					this.conn.commit();
				}
			} catch (final Throwable t) {
				Report.exception("TRANSACTION-JDBC", "while commiting invalidations", t);
			}
			try {
				this.closed = true;
				this.conn.close();
			} catch (final Throwable t) {
				// ignore
			}
			this.server.stsTransactionTime.register(System.currentTimeMillis() - this.started);
		}
	}

	@Override
	public final void create(final boolean local,
			final String ctnLnkId,
			final String lnkId,
			final String objId,
			final String name,
			final boolean folder,
			final long created,
			final String owner,
			final int state,
			final String title,
			final String typeName,
			final BaseObject added,
			final String vrId,
			final String vrComment,
			final BaseObject vrData) throws Throwable {

		this.invalidateUpdateLink(lnkId);
		this.invalidateUpdateTree(ctnLnkId);
		this.invalidateUpdateIden(objId);
		Map<String, String> extraExisting = null;
		if (added != null) {
			extraExisting = Differer.getExtraDiff(extraExisting, added, "", this.issuer);
		}
		if (vrId == null || "*".equals(vrId)) {
			MatData.serializeCreate(this.server, this.conn, objId, "*", title, created, typeName, owner, state, extraExisting, added);
		} else {
			this.versionCreate(vrId, "*", vrComment, objId, title, typeName, owner, vrData);
			MatData.serializeCreate(this.server, this.conn, objId, vrId, title, created, typeName, owner, state, extraExisting, added);
		}
		MatLink.serializeCreate(this.conn, ctnLnkId, lnkId, name, folder, objId);
		MatChange.serialize(
				this.server,
				this.conn,
				0,
				local
					? "create"
					: "create-global",
				lnkId,
				-1);
	}

	@Override
	public final void delete(final LinkData link, final boolean soft) throws Throwable {

		final Collection<LinkData> links = MatLink.searchLinks(this.conn, link.objId, true);
		if (links == null) {
			throw new IllegalStateException("Cannot delete non existent object from storage! Should I be just a warning entry in a log file?");
		}
		this.invalidateUpdateIden(link.objId);
		for (final LinkData current : links) {
			if (soft) {
				MatAlias.stayOrMove(this.conn, current.lnkLuid);
			} else {
				MatAlias.deleteOrMove(this.conn, current.lnkLuid);
			}
			this.invalidateDeleteLink(current.lnkId);
			this.invalidateUpdateTree(current.lnkCntId);
			if (soft) {
				MatLink.recycle(this.conn, current.lnkId);
				MatChange.serialize(this.server, this.conn, 5, "recycle-start", current.lnkId, current.lnkLuid);
			} else {
				MatLink.unlink(this.conn, current.lnkId);
				MatChange.serialize(this.server, this.conn, 10, "clean", link.objId, current.lnkLuid);
				MatChange.serialize(this.server, this.conn, 15, "clean-start", current.lnkId, current.lnkLuid);
			}
		}
	}

	@Override
	protected final void finalize() throws Throwable {

		super.finalize();
		this.rollback();
	}

	private final void invalidateDeleteLink(final String lnkId) {

		if (lnkId == null) {
			throw new NullPointerException("Null argument!");
		}
		if (this.invalidations == null) {
			this.invalidations = new InvalidationCollector();
		}
		this.invalidations.add(InvalidationEventType.DLINK, lnkId);
	}

	private final void invalidateUpdateIden(final String lnkId) {

		if (lnkId == null) {
			throw new NullPointerException("Null argument!");
		}
		if (this.invalidations == null) {
			this.invalidations = new InvalidationCollector();
		}
		this.invalidations.add(InvalidationEventType.UIDEN, lnkId);
	}

	private final void invalidateUpdateLink(final String lnkId) {

		if (lnkId == null) {
			throw new NullPointerException("Null argument!");
		}
		if (this.invalidations == null) {
			this.invalidations = new InvalidationCollector();
		}
		this.invalidations.add(InvalidationEventType.ULINK, lnkId);
	}

	private final void invalidateUpdateTree(final String lnkId) {

		if (lnkId == null) {
			throw new NullPointerException("Null argument!");
		}
		if (this.invalidations == null) {
			this.invalidations = new InvalidationCollector();
		}
		this.invalidations.add(InvalidationEventType.UTREE, lnkId);
	}

	@Override
	public final void link(final boolean local, final String ctnLnkId, final String lnkId, final String name, final boolean folder, final String objId) throws Throwable {

		this.invalidateUpdateTree(ctnLnkId);
		this.invalidateUpdateIden(objId);
		this.invalidateUpdateLink(lnkId);
		final Collection<LinkData> links = MatLink.searchLinks(this.conn, objId, true);
		if (links != null) {
			for (final LinkData current : links) {
				this.invalidateUpdateLink(current.lnkId);
			}
		}
		MatLink.serializeCreate(this.conn, ctnLnkId, lnkId, name, folder, objId);
		MatChange.serialize(
				this.server,
				this.conn,
				0,
				local
					? "create"
					: "create-global",
				lnkId,
				-1);
	}

	@Override
	public final void move(final LinkData link, final String cntLnkId, final String key) throws Throwable {

		this.invalidateUpdateLink(link.lnkId);
		this.invalidateUpdateTree(link.lnkCntId);
		this.invalidateUpdateIden(link.objId);
		this.invalidateUpdateTree(cntLnkId);
		MatLink.move(this.conn, link.lnkCntId, link.lnkName, cntLnkId, key);
		MatChange.serialize(this.server, this.conn, 0, "update", link.lnkId, link.lnkLuid);
	}

	@Override
	public final void record(final String linkedIdentity) throws Throwable {

		MatHistory.record(this.conn, linkedIdentity);
	}

	@Override
	public final void rename(final LinkData link, final String key) throws Throwable {

		this.invalidateUpdateLink(link.lnkId);
		this.invalidateUpdateTree(link.lnkCntId);
		MatLink.rename(this.conn, link.lnkCntId, link.lnkName, key);
		MatChange.serialize(this.server, this.conn, 0, "update", link.lnkId, link.lnkLuid);
	}

	@Override
	public final void resync(final String lnkId) throws Throwable {

		MatChange.serialize(this.server, this.conn, 0, "resync", lnkId, -1);
	}

	@Override
	public final void revert(final LinkData link, final String historyId, final boolean folder, final long created, final int state, final String title, final String typeName)
			throws Throwable {

		this.invalidateUpdateLink(link.lnkId);
		this.invalidateUpdateTree(link.lnkCntId);
		this.invalidateUpdateIden(link.objId);
		final Collection<LinkData> links = MatLink.searchLinks(this.conn, link.objId, true);
		if (links != null) {
			for (final LinkData current : links) {
				this.invalidateUpdateLink(current.lnkId);
			}
		}
		MatLink.update(this.conn, link.lnkId, link.lnkFolder, folder);
		MatData.update(
				this.server,
				this.conn,
				historyId,
				link.objId,
				"*",
				link.objTitle,
				title,
				link.objCreated,
				created,
				link.objType,
				typeName,
				Context.getUserId(Exec.currentProcess()),
				link.objState,
				state,
				null,
				null,
				null,
				null);
		MatChange.serialize(this.server, this.conn, 0, "update-all", link.objId, link.lnkLuid);
	}

	@Override
	public final void revert(final LinkData link,
			final String historyId,
			final boolean folder,
			final long created,
			final int state,
			final String title,
			final String typeName,
			final BaseObject removed,
			final BaseObject added) throws Throwable {

		this.invalidateUpdateLink(link.lnkId);
		this.invalidateUpdateTree(link.lnkCntId);
		this.invalidateUpdateIden(link.objId);
		final Collection<LinkData> links = MatLink.searchLinks(this.conn, link.objId, true);
		if (links != null) {
			for (final LinkData current : links) {
				this.invalidateUpdateLink(current.lnkId);
			}
		}
		Map<String, String> extraRemoved = null;
		if (removed != null) {
			extraRemoved = Differer.getExtraRemoved(removed, this.issuer);
		}
		Map<String, String> extraExisting = null;
		if (added != null) {
			extraExisting = Differer.getExtraDiff(extraExisting, added, "", this.issuer);
		}
		MatLink.update(this.conn, link.lnkId, link.lnkFolder, folder);
		MatData.update(
				this.server,
				this.conn,
				historyId,
				link.objId,
				"*",
				link.objTitle,
				title,
				link.objCreated,
				created,
				link.objType,
				typeName,
				Context.getUserId(Exec.currentProcess()),
				link.objState,
				state,
				extraRemoved,
				extraExisting,
				removed,
				added);
		MatChange.serialize(this.server, this.conn, 0, "update-all", link.objId, link.lnkLuid);
	}

	@Override
	public final void rollback() {

		if (this.closed) {
			return;
		}
		try {
			this.conn.rollback();
			this.server.stsTransactionsRollbacks++;
		} catch (final SQLException e) {
			throw new RuntimeException(e);
		} finally {
			try {
				if (this.invalidations != null) {
					MatChange.serializeInvalidations(this.server.getStorage(), this.conn, this.invalidations);
					this.invalidations = null;
					this.conn.commit();
				}
			} catch (final Throwable t) {
				Report.exception("TRANSACTION-JDBC", "while commiting invalidations", t);
			}
			try {
				this.closed = true;
				this.conn.close();
			} catch (final Throwable t) {
				// ignore
			}
			this.server.stsTransactionTime.register(System.currentTimeMillis() - this.started);
		}
	}

	@Override
	public final void segregate(final String guid, final String linkedIdentityOld, final String linkedIdentityNew) throws Throwable {

		this.invalidateUpdateLink(guid);
		this.invalidateUpdateIden(linkedIdentityNew);
		this.invalidateUpdateIden(linkedIdentityOld);
		final Collection<LinkData> links = MatLink.searchLinks(this.conn, linkedIdentityOld, true);
		if (links != null) {
			for (final LinkData current : links) {
				this.invalidateUpdateLink(current.lnkId);
			}
		}
		try (final PreparedStatement ps = this.conn.prepareStatement(
				"INSERT INTO s3Objects(objId,vrId,objTitle,objCreated,objDate,objOwner,objType,objState,extLink) SELECT ?,?,objTitle,objCreated,objDate,objOwner,objType,objState,extLink FROM s3Objects WHERE objId=?")) {
			ps.setString(1, linkedIdentityNew);
			ps.setString(2, "*");
			ps.setString(3, linkedIdentityOld);
			ps.executeUpdate();
		}
		try (final PreparedStatement ps = this.conn.prepareStatement("INSERT INTO s3ExtraLink(objId,fldId,recId) SELECT ?,fldId,recId FROM s3ExtraLink WHERE objId=?")) {
			ps.setString(1, linkedIdentityNew);
			ps.setString(2, linkedIdentityOld);
			ps.executeUpdate();
		}
		try (final PreparedStatement ps = this.conn.prepareStatement("UPDATE s3Tree SET objId=? WHERE lnkId=? AND objId=?")) {
			ps.setString(1, linkedIdentityNew);
			ps.setString(2, guid);
			ps.setString(3, linkedIdentityOld);
			ps.executeUpdate();
		}
	}

	@Override
	public final String toString() {

		return "Transaction jdbc{srv=" + this.server + "}";
	}

	@Override
	public final void unlink(final LinkData link, final boolean soft) throws Throwable {

		this.invalidateDeleteLink(link.lnkId);
		if (link.lnkCntId != null) {
			this.invalidateUpdateTree(link.lnkCntId);
		}
		this.invalidateUpdateIden(link.objId);
		final Collection<LinkData> links = MatLink.searchLinks(this.conn, link.objId, true);
		if (links != null) {
			for (final LinkData current : links) {
				this.invalidateUpdateLink(current.lnkId);
			}
		}
		if (soft) {
			MatAlias.stayOrMove(this.conn, link.lnkLuid);
			MatLink.recycle(this.conn, link.lnkId);
			MatChange.serialize(this.server, this.conn, 0, "recycle-start", link.lnkId, link.lnkLuid);
		} else {
			MatAlias.deleteOrMove(this.conn, link.lnkLuid);
			MatLink.unlink(this.conn, link.lnkId);
			MatChange.serialize(this.server, this.conn, 10, "clean", link.objId, link.lnkLuid);
			MatChange.serialize(this.server, this.conn, 15, "clean-start", link.lnkId, link.lnkLuid);
		}
	}

	@Override
	public final void update(final LinkData link, final String objId) throws Throwable {

		this.invalidateUpdateLink(link.lnkId);
		this.invalidateUpdateTree(link.lnkCntId);
		this.invalidateUpdateIden(objId);
		MatData.update(this.conn, objId);
		final Collection<LinkData> links = MatLink.searchLinks(this.conn, objId, true);
		if (links != null) {
			for (final LinkData current : links) {
				this.invalidateUpdateLink(current.lnkId);
				if (current.lnkCntId != null) {
					this.invalidateUpdateTree(current.lnkCntId);
				}
			}
		}
		MatChange.serialize(this.server, this.conn, 0, "update", link.lnkId, link.lnkLuid);
	}

	@Override
	public final void update(final LinkData link,
			final String objId,
			final String versionId,
			final boolean folder,
			final long created,
			final int state,
			final String title,
			final String typeName,
			final boolean ownership) throws Throwable {

		this.invalidateUpdateIden(objId);
		MatData.update(
				this.conn,
				objId,
				versionId,
				link.objTitle,
				title,
				link.objCreated,
				created,
				link.objType,
				typeName,
				ownership
					? Context.getUserId(Exec.currentProcess())
					: null,
				link.objState,
				state);
		this.invalidateUpdateLink(link.lnkId);
		this.invalidateUpdateTree(link.lnkCntId);
		MatLink.update(this.conn, link.lnkId, link.lnkFolder, folder);
		final Collection<LinkData> links = MatLink.searchLinks(this.conn, objId, true);
		if (links != null) {
			for (final LinkData current : links) {
				this.invalidateUpdateLink(current.lnkId);
				if (current.lnkCntId != null) {
					this.invalidateUpdateTree(current.lnkCntId);
				}
			}
		}
		Report.info("S3-TRANSACTION", "update short: type=" + typeName);
		MatChange.serialize(this.server, this.conn, 0, "update-all", objId, link.lnkLuid);
	}

	@Override
	public final void update(final LinkData link,
			final String objId,
			final String versionId,
			final boolean folder,
			final long created,
			final int state,
			final String title,
			final String typeName,
			final boolean ownership,
			final BaseObject removed,
			final BaseObject added) throws Throwable {

		Map<String, String> extraRemoved = null;
		if (removed != null) {
			extraRemoved = Differer.getExtraRemoved(removed, this.issuer);
		}
		Map<String, String> extraExisting = null;
		if (added != null) {

			extraExisting = Differer.getExtraDiff(extraExisting, added, "", this.issuer);
		}
		this.invalidateUpdateIden(objId);
		MatData.update(
				this.server,
				this.conn,
				null,
				objId,
				versionId,
				link.objTitle,
				title,
				link.objCreated,
				created,
				link.objType,
				typeName,
				ownership
					? Context.getUserId(Exec.currentProcess())
					: null,
				link.objState,
				state,
				extraRemoved,
				extraExisting,
				removed,
				added);
		this.invalidateUpdateLink(link.lnkId);
		this.invalidateUpdateTree(link.lnkCntId);
		final Collection<LinkData> links = MatLink.searchLinks(this.conn, objId, true);
		if (links != null) {
			for (final LinkData current : links) {
				this.invalidateUpdateLink(current.lnkId);
				if (current.lnkCntId != null) {
					this.invalidateUpdateTree(current.lnkCntId);
				}
			}
		}
		MatLink.update(this.conn, link.lnkId, link.lnkFolder, folder);
		Report.info("S3-TRANSACTION", "update full: type=" + typeName + ", name=" + link.lnkName);
		MatChange.serialize(this.server, this.conn, 0, "update-all", objId, link.lnkLuid);
	}

	@Override
	public final void versionClearAll(final String objId) throws Throwable {

		MatVersion.clear(this.conn, objId);
	}

	@Override
	public final void versionCreate(final String vrId,
			final String vrParentId,
			final String vrComment,
			final String objId,
			final String title,
			final String typeName,
			final String owner,
			final BaseObject vrData) throws Throwable {

		final Map<String, String> versionExtra = Differer.getExtraDiff(null, vrData, "", this.issuer);
		MatVersion.serializeCreate(this.server, this.conn, vrId, vrParentId, vrComment, objId, title, owner, typeName, versionExtra, vrData);
	}

	@Override
	public final void
			versionStart(final String vrId, final String vrComment, final String objId, final String title, final String typeName, final String owner, final BaseObject vrData)
					throws Throwable {

		this.versionCreate(vrId, "*", vrComment, objId, title, typeName, owner, vrData);
	}
}
