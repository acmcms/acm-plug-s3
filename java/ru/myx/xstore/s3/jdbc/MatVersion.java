package ru.myx.xstore.s3.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import ru.myx.ae1.storage.BaseVersion;
import ru.myx.ae3.Engine;
import ru.myx.ae3.base.Base;
import ru.myx.ae3.base.BaseNativeObject;
import ru.myx.ae3.base.BaseObject;
import ru.myx.ae3.binary.TransferCopier;
import ru.myx.ae3.common.Value;
import ru.myx.ae3.help.Convert;
import ru.myx.ae3.help.Text;
import ru.myx.ae3.report.Report;
import ru.myx.ae3.xml.Xml;

final class MatVersion {

	static final void clear(final Connection conn, final String objId) throws Exception {

		try (final PreparedStatement ps = conn.prepareStatement("DELETE FROM s3ObjectVersions WHERE objId=?")) {
			ps.setString(1, objId);
			ps.execute();
		}
	}

	static final BaseVersion[] materialize(final Connection conn, final String objId) throws Exception {

		try (final PreparedStatement ps = conn.prepareStatement(
				"SELECT vrId,vrDate,vrParentId,vrComment,vrTitle,vrOwner,vrType FROM s3ObjectVersions WHERE objId=? ORDER BY vrDate ASC",
				ResultSet.TYPE_FORWARD_ONLY,
				ResultSet.CONCUR_READ_ONLY)) {
			ps.setString(1, objId);
			try (final ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					final List<BaseVersion> result = new ArrayList<>();
					do {
						result.add(
								new VersionJdbc(
										rs.getString(1),
										rs.getTimestamp(2).getTime(),
										rs.getString(3),
										rs.getString(4),
										rs.getString(5),
										rs.getString(6),
										rs.getString(7)));
					} while (rs.next());
					return result.toArray(new BaseVersion[result.size()]);
				}
				return null;
			}
		}
	}

	static final BaseObject materializeSnapshot(final ServerJdbc server, final Connection conn, final String objId, final String versionId) throws Exception {

		try (final PreparedStatement ps = conn.prepareStatement(
				"SELECT o.extLink,v.extLink FROM s3ObjectVersions v,s3Objects o WHERE v.objId=o.objId AND v.vrId=? AND o.objId=?",
				ResultSet.TYPE_FORWARD_ONLY,
				ResultSet.CONCUR_READ_ONLY)) {
			ps.setString(1, versionId);
			ps.setString(2, objId);
			try (final ResultSet rs = ps.executeQuery()) {
				final BaseObject data = new BaseNativeObject(null);
				if (rs.next()) {
					final String extLinkObject = rs.getString(1);
					if (extLinkObject != null && !extLinkObject.equals("*")) {
						final Object o = server.getStorageExternalizerObject().getExternal(null, extLinkObject);
						if (o != null) {
							final BaseObject map = Convert.Any.toAny(((Value<?>) o).baseValue());
							if (map != null) {
								data.baseDefineImportAllEnumerable(map);
							}
						}
					}
					final String extLinkVersion = rs.getString(2);
					if (extLinkVersion != null && !extLinkVersion.equals("*")) {
						final Object o = server.getStorageExternalizerObject().getExternal(null, extLinkVersion);
						if (o != null) {
							final BaseObject map = Convert.Any.toAny(((Value<?>) o).baseValue());
							if (map != null) {
								data.baseDefineImportAllEnumerable(map);
							}
						}
					}
					return data;
				}
				return null;
			}
		}
	}

	static final void serializeCreate(

			final ServerJdbc server,
			final Connection conn,
			final String vrId,
			final String vrParentId,
			final String vrComment,
			final String objId,
			final String vrTitle,
			final String vrOwner,
			final String vrType,
			final Map<String, String> vrExtra,
			final BaseObject vrData) throws Exception {

		{
			assert vrData != null : "NULL java object!";
			try (final PreparedStatement ps = conn
					.prepareStatement("INSERT INTO s3ObjectVersions(vrId,vrDate,vrParentId,vrComment,objId,vrTitle,vrOwner,vrType,extLink) VALUES (?,?,?,?,?,?,?,?,?)")) {
				ps.setString(1, vrId);
				ps.setTimestamp(2, new Timestamp(Engine.fastTime()));
				ps.setString(3, vrParentId);
				ps.setString(4, vrComment);
				ps.setString(5, objId);
				ps.setString(6, vrTitle);
				ps.setString(7, vrOwner);
				ps.setString(8, vrType);
				if (!Base.hasKeys(vrData)) {
					ps.setString(9, "*");
				} else {
					final TransferCopier binary = Xml.toXmlBinary("data", vrData, false, server.getStorageExternalizerObject(), new StoreInfo(conn, objId, true), 4096);
					final String guid = server.getStorageExternalizerObject().putExternal(new StoreInfo(conn, objId, true), "$data", "text/xml", binary);
					ps.setString(9, guid);
				}
				ps.execute();
			}
		}
		if (vrExtra != null && !vrExtra.isEmpty()) {
			for (final Map.Entry<String, String> record : vrExtra.entrySet()) {
				final String fldId = record.getKey();
				final String recId = record.getValue();
				try (final PreparedStatement ps = conn.prepareStatement("INSERT INTO s3ExtraLink(recId,objId,fldId) VALUES (?,?,?) ")) {
					ps.setString(1, recId);
					ps.setString(2, vrId);
					ps.setString(3, Text.limitString(fldId, 32));
					ps.execute();
				} catch (final Throwable t) {
					Report.exception("S3/JDBC/MAT_DATA", "Exception while linking extra record to a version", t);
				}
			}
		}
	}
}
