/*
 * Created on 12.07.2004
 */
package ru.myx.xstore.s3.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import ru.myx.ae3.Engine;
import ru.myx.ae3.base.Base;
import ru.myx.ae3.base.BaseObject;
import ru.myx.ae3.base.BaseProperty;
import ru.myx.ae3.binary.TransferCopier;
import ru.myx.ae3.common.Value;
import ru.myx.ae3.common.WaitTimeoutException;
import ru.myx.ae3.extra.ExternalHandler;
import ru.myx.ae3.help.Convert;
import ru.myx.ae3.help.Text;
import ru.myx.ae3.report.Report;
import ru.myx.ae3.xml.Xml;

/** @author myx
 *
 *
 *         To change the template for this generated type comment go to
 *         Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments */
final class MatData {

	static final void dataSerialize(final ServerJdbc server,
			final Connection conn,
			final String objId,
			final PreparedStatement ps,
			final int index,
			final BaseObject data,
			final boolean preferInsert) throws Exception {

		assert data != null : "NULL java object!";
		if (!Base.hasKeys(data)) {
			ps.setString(index, "*");
		} else {
			final ExternalHandler storageExternalizerObject = server.getStorageExternalizerObject();
			final TransferCopier binary = Xml.toXmlBinary("data", data, false, storageExternalizerObject, new StoreInfo(conn, objId, false), 4096);
			final String guid = storageExternalizerObject.putExternal(new StoreInfo(conn, objId, preferInsert), "$data", "text/xml", binary);
			ps.setString(index, guid);
		}
	}

	static final void delete(final Connection conn, final String objId) throws Exception {

		try (final PreparedStatement ps = conn.prepareStatement("DELETE FROM s3Objects WHERE objId=?")) {
			ps.setString(1, objId);
			ps.execute();
		}
		try (final PreparedStatement ps = conn.prepareStatement("DELETE FROM s3ExtraLink WHERE objId=?")) {
			ps.setString(1, objId);
			ps.execute();
		}
	}

	/** @param server
	 * @param conn
	 * @param objId
	 * @param historyId
	 * @return
	 * @throws SQLException
	 * @throws IllegalArgumentException
	 * @throws Exception
	 * @throws WaitTimeoutException
	 */
	private static BaseObject readDataMap(final ServerJdbc server, final Connection conn, final String objId, final String historyId)
			throws SQLException, IllegalArgumentException, Exception, WaitTimeoutException {

		final String extLink;
		try (final PreparedStatement ps = conn.prepareStatement(
				historyId == null
					? "SELECT extLink FROM s3Objects WHERE objId=?"
					: "SELECT extLink FROM s3ObjectHistory WHERE objId=? AND hsId=?",
				ResultSet.TYPE_FORWARD_ONLY,
				ResultSet.CONCUR_READ_ONLY)) {
			ps.setString(1, objId);
			if (historyId != null) {
				ps.setString(2, historyId);
			}
			try (final ResultSet rs = ps.executeQuery()) {
				if (!rs.next()) {
					throw new IllegalArgumentException("No entry found to update, objId=" + objId);
				}
				extLink = rs.getString(1);
			}
		}
		if (extLink == null || extLink.equals("*")) {
			return BaseObject.UNDEFINED;
		}
		final Object o = server.getStorageExternalizerObject().getExternal(new StoreInfo(conn, extLink, false), extLink);
		if (o == null) {
			return BaseObject.UNDEFINED;
		}
		final BaseObject map = Convert.Any.toAny(((Value<?>) o).baseValue());
		return map == null
			? BaseObject.UNDEFINED
			: map;
	}

	static final void serializeCreate(final ServerJdbc server,
			final Connection conn,
			final String objId,
			final String vrId,
			final String title,
			final long created,
			final String typeName,
			final String owner,
			final int state,
			final Map<String, String> extraExisting,
			final BaseObject data) throws Exception {

		assert data != null : "NULL java object!";
		try (final PreparedStatement ps = conn
				.prepareStatement("INSERT INTO s3Objects(objId,vrId,objTitle,objCreated,objDate,objOwner,objType,objState,extLink) VALUES (?,?,?,?,?,?,?,?,?)")) {
			ps.setString(1, objId);
			ps.setString(2, vrId);
			ps.setString(3, title);
			ps.setTimestamp(4, new Timestamp(created));
			ps.setTimestamp(5, new Timestamp(Engine.fastTime()));
			ps.setString(6, owner);
			ps.setString(7, typeName);
			ps.setInt(8, state);
			MatData.dataSerialize(server, conn, objId, ps, 9, data, true);
			ps.execute();
		}
		if (extraExisting != null && !extraExisting.isEmpty()) {
			try (final PreparedStatement ps = conn.prepareStatement("INSERT INTO s3ExtraLink(recId,objId,fldId) VALUES (?,?,?) ")) {
				for (final Map.Entry<String, String> record : extraExisting.entrySet()) {
					final String fldId = record.getKey();
					final String recId = record.getValue();
					try {
						ps.setString(1, recId);
						ps.setString(2, objId);
						ps.setString(3, Text.limitString(fldId, 32));
						ps.execute();
						ps.clearParameters();
					} catch (final Throwable t) {
						Report.exception("S3/JDBC/MAT_DATA", "CREATE: while linking extra record to an object, objId=" + objId + ", fldId=" + fldId + ", recId=" + recId, t);
					}
				}
			}
		}
	}

	static final void update(final Connection conn, final String objId) throws Exception {

		try (final PreparedStatement ps = conn.prepareStatement("UPDATE s3Objects SET objDate=? WHERE objId=?")) {
			ps.setTimestamp(1, new Timestamp(Engine.fastTime()));
			ps.setString(2, objId);
			ps.execute();
		}
	}

	static final void update(final Connection conn,
			final String objId,
			final String vrId,
			final String initialTitle,
			final String title,
			final long initialCreated,
			final long created,
			final String initialTypeName,
			final String typeName,
			final String owner,
			final int oldState,
			final int newState) throws Exception {

		final List<String> setPart = new ArrayList<>();
		if (oldState != newState) {
			setPart.add("objState=?");
		}
		final boolean setTitle = initialTitle != title && !initialTitle.equals(title);
		if (setTitle) {
			setPart.add("objTitle=?");
		}
		final boolean setCreated = initialCreated != created;
		if (setCreated) {
			setPart.add("objCreated=?");
		}
		final boolean setTypeName = initialTypeName != typeName && !initialTypeName.equals(typeName);
		if (setTypeName) {
			setPart.add("objType=?");
		}
		if (owner != null) {
			setPart.add("objOwner=?");
		}
		if (!setPart.isEmpty()) {
			try (final PreparedStatement ps = conn.prepareStatement("UPDATE s3Objects SET objDate=?, vrId=?, " + Text.join(setPart, ", ") + " WHERE objId=?")) {
				int index = 1;
				ps.setTimestamp(index++, new Timestamp(Engine.fastTime()));
				ps.setString(index++, vrId);
				if (oldState != newState) {
					ps.setInt(index++, newState);
				}
				if (setTitle) {
					ps.setString(index++, title);
				}
				if (setCreated) {
					ps.setTimestamp(index++, new Timestamp(created));
				}
				if (setTypeName) {
					ps.setString(index++, typeName);
				}
				if (owner != null) {
					ps.setString(index++, owner);
				}
				ps.setString(index, objId);
				ps.execute();
			}
		}
	}

	static final void update(final ServerJdbc server,
			final Connection conn,
			final String historyId,
			final String objId,
			final String vrId,
			final String initialTitle,
			final String title,
			final long initialCreated,
			final long created,
			final String initialTypeName,
			final String typeName,
			final String owner,
			final int oldState,
			final int newState,
			final Map<String, String> extraRemoved,
			final Map<String, String> extraExisting,
			final BaseObject removed,
			final BaseObject added) throws Exception {

		final List<String> setPart = new ArrayList<>();
		if (historyId != null || oldState != newState) {
			setPart.add("objState=?");
		}
		final boolean setTitle = historyId != null || initialTitle != title && !initialTitle.equals(title);
		if (setTitle) {
			setPart.add("objTitle=?");
		}
		final boolean setCreated = historyId != null || initialCreated != created;
		if (setCreated) {
			setPart.add("objCreated=?");
		}
		final boolean setTypeName = historyId != null || initialTypeName != typeName && !initialTypeName.equals(typeName);
		if (setTypeName) {
			setPart.add("objType=?");
		}
		if (owner != null) {
			setPart.add("objOwner=?");
		}
		final BaseObject data;
		if (historyId != null || removed.baseHasKeysOwn() || added.baseHasKeysOwn()) {
			if (extraRemoved != null && !extraRemoved.isEmpty()) {
				for (final Map.Entry<String, String> record : extraRemoved.entrySet()) {
					final String fldId = record.getKey();
					final String recId = record.getValue();
					try (final PreparedStatement ps = conn.prepareStatement("DELETE FROM s3ExtraLink WHERE recId=? AND objId=? AND fldId=?")) {
						ps.setString(1, recId);
						ps.setString(2, objId);
						ps.setString(3, Text.limitString(fldId, 32));
						ps.execute();
					} catch (final Throwable t) {
						Report.exception("S3/JDBC/MAT_DATA", "Exception while unlinking extra record to an object", t);
					}
				}
			}
			final BaseObject dataMap = MatData.readDataMap(server, conn, objId, historyId);
			if (dataMap == BaseObject.UNDEFINED) {
				if (added.baseHasKeysOwn()) {
					data = added;
				} else {
					data = null;
				}
			} else {
				{
					final Iterator<String> iterator = removed.baseKeysOwn();
					while (iterator.hasNext()) {
						final String key = iterator.next();
						dataMap.baseDelete(key);
					}
				}
				{
					final Iterator<String> iterator = added.baseKeysOwn();
					while (iterator.hasNext()) {
						final String key = iterator.next();
						dataMap.baseDefine(key, added.baseGet(key, BaseObject.UNDEFINED), BaseProperty.ATTRS_MASK_WED);
					}
				}
				data = dataMap;
			}
			setPart.add("extLink=?");
		} else {
			data = null;
		}
		if (!setPart.isEmpty()) {
			try (final PreparedStatement ps = conn.prepareStatement("UPDATE s3Objects SET objDate=?, vrId=?, " + Text.join(setPart, ", ") + " WHERE objId=?")) {
				int index = 1;
				ps.setTimestamp(index++, new Timestamp(Engine.fastTime()));
				ps.setString(index++, vrId);
				if (historyId != null || oldState != newState) {
					ps.setInt(index++, newState);
				}
				if (setTitle) {
					ps.setString(index++, title);
				}
				if (setCreated) {
					ps.setTimestamp(index++, new Timestamp(created));
				}
				if (setTypeName) {
					ps.setString(index++, typeName);
				}
				if (owner != null) {
					ps.setString(index++, owner);
				}
				if (data != null) {
					MatData.dataSerialize(server, conn, objId, ps, index, data, false);
					index++;
				}
				ps.setString(index, objId);
				ps.execute();
			}
		}
		if (historyId != null) {
			try (final PreparedStatement ps = conn.prepareStatement("DELETE FROM s3ExtraLink WHERE objId=?")) {
				ps.setString(1, objId);
				ps.execute();
			}
			try (final PreparedStatement ps = conn.prepareStatement("INSERT INTO s3ExtraLink(objId,fldId,recId) SELECT ?, fldId, recId FROM s3ExtraLink WHERE objId=?")) {
				ps.setString(1, objId);
				ps.setString(2, historyId);
				ps.execute();
			}
		}
		if (extraExisting != null && !extraExisting.isEmpty()) {
			for (final Map.Entry<String, String> record : extraExisting.entrySet()) {
				final String fldId = record.getKey();
				final String recId = record.getValue();
				final String fldIdPrepared = Text.limitString(fldId, 32);
				try (final PreparedStatement ps = conn.prepareStatement("DELETE FROM s3ExtraLink WHERE objId=? AND fldId=?")) {
					ps.setString(1, objId);
					ps.setString(2, fldIdPrepared);
					ps.execute();
				} catch (final Throwable t) {
					Report.exception("S3/JDBC/MAT_DATA", "UPDATE: while preparing to link extra record to an object, objId=" + objId + ", fldId=" + fldId, t);
				}
				try (final PreparedStatement ps = conn.prepareStatement("INSERT INTO s3ExtraLink(recId,objId,fldId) VALUES (?,?,?)")) {
					ps.setString(1, recId);
					ps.setString(2, objId);
					ps.setString(3, fldIdPrepared);
					ps.execute();
				} catch (final Throwable t) {
					Report.exception("S3/JDBC/MAT_DATA", "UPDATE: while linking extra record to an object, objId=" + objId + ", fldId=" + fldId + ", recId=" + recId, t);
				}
			}
		}
	}
}
