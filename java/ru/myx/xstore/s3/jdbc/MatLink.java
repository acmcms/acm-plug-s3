/*
 * Created on 12.07.2004
 */
package ru.myx.xstore.s3.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import ru.myx.ae1.storage.ModuleInterface;
import ru.myx.ae3.Engine;
import ru.myx.ae3.act.Context;
import ru.myx.ae3.exec.Exec;
import ru.myx.ae3.help.Create;
import ru.myx.ae3.help.Text;
import ru.myx.ae3.report.Report;
import ru.myx.xstore.s3.concept.LinkData;

/** @author myx
 *
 *
 *         To change the template for this generated type comment go to
 *         Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments */
final class MatLink {

	private static final int STATE_SYSTEM = ModuleInterface.STATE_SYSTEM;

	private static final int STATE_ARCHIVE = ModuleInterface.STATE_ARCHIVE;

	private static final int STATE_PUBLISH = ModuleInterface.STATE_PUBLISH;

	static final LinkData materializeActual(final Connection conn, final String guid) throws Exception {

		try (final PreparedStatement ps = conn.prepareStatement(
				"SELECT t.lnkLuid,t.cntLnkId,t.lnkName,t.lnkFolder,t.objId,o.vrId,o.objTitle,o.objCreated,o.objDate,o.objOwner,o.objType,o.objState,o.extLink FROM s3Tree t,s3Objects o WHERE t.objId=o.objId AND t.lnkId=?",
				ResultSet.TYPE_FORWARD_ONLY,
				ResultSet.CONCUR_READ_ONLY)) {
			ps.setString(1, guid);
			try (final ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					final int lnkLuid = rs.getInt(1);
					final String lnkCntId = rs.getString(2);
					final String lnkName = rs.getString(3);
					final boolean lnkFolder = "Y".equals(rs.getString(4));
					final String objId = rs.getString(5);
					final String vrId = rs.getString(6);
					final String objTitle = rs.getString(7);
					final long objCreated = rs.getTimestamp(8).getTime();
					final long objDate = rs.getTimestamp(9).getTime();
					final String objOwner = rs.getString(10);
					final String objType = rs.getString(11);
					final int objState = rs.getInt(12);
					final boolean trListable = objState == MatLink.STATE_SYSTEM || objState == MatLink.STATE_PUBLISH;
					final boolean trSearchable = objState == MatLink.STATE_ARCHIVE || objState == MatLink.STATE_PUBLISH;
					final String extLink = rs.getString(13);
					return new LinkData(
							guid,
							lnkLuid,
							lnkCntId,
							lnkName,
							lnkFolder,
							objId,
							vrId,
							objTitle,
							objCreated,
							objDate,
							objOwner,
							objType,
							objState,
							trListable,
							trSearchable,
							extLink);
				}
				return null;
			}
		}
	}

	static final LinkData materializeHistory(final Connection conn, final String guid, final String historyId) throws Exception {

		try (final PreparedStatement ps = conn.prepareStatement(
				"SELECT t.lnkLuid,t.cntLnkId,t.lnkName,t.lnkFolder,t.objId,o.vrId,o.objTitle,o.objCreated,o.objDate,o.objOwner,o.objType,o.objState,o.extLink FROM s3Tree t,s3ObjectHistory o WHERE t.objId=o.objId AND t.lnkId=? AND o.hsId=?",
				ResultSet.TYPE_FORWARD_ONLY,
				ResultSet.CONCUR_READ_ONLY)) {
			ps.setString(1, guid);
			ps.setString(2, historyId);
			try (final ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					final int lnkLuid = rs.getInt(1);
					final String lnkCntId = rs.getString(2);
					final String lnkName = rs.getString(3);
					final boolean lnkFolder = "Y".equals(rs.getString(4));
					final String objId = rs.getString(5);
					final String vrId = rs.getString(6);
					final String objTitle = rs.getString(7);
					final long objCreated = rs.getTimestamp(8).getTime();
					final long objDate = rs.getTimestamp(9).getTime();
					final String objOwner = rs.getString(10);
					final String objType = rs.getString(11);
					final int objState = rs.getInt(12);
					final boolean trListable = objState == MatLink.STATE_SYSTEM || objState == MatLink.STATE_PUBLISH;
					final boolean trSearchable = objState == MatLink.STATE_ARCHIVE || objState == MatLink.STATE_PUBLISH;
					final String extLink = rs.getString(13);
					return new LinkData(
							guid,
							lnkLuid,
							lnkCntId,
							lnkName,
							lnkFolder,
							objId,
							vrId,
							objTitle,
							objCreated,
							objDate,
							objOwner,
							objType,
							objState,
							trListable,
							trSearchable,
							extLink);
				}
				return null;
			}
		}
	}

	static final LinkData materializeVersion(final Connection conn, final String guid, final String versionId) throws Exception {

		try (final PreparedStatement ps = conn.prepareStatement(
				"SELECT t.lnkLuid,t.cntLnkId,t.lnkName,t.lnkFolder,t.objId,v.vrId,v.objTitle,v.objCreated,v.objDate,v.objOwner,v.objType,o.objState,v.extLink FROM s3Tree t,s3Objects o,s3ObjectVersions v WHERE t.objId=o.objId AND t.lnkId=? AND o.objId=v.objId AND v.vrId=?",
				ResultSet.TYPE_FORWARD_ONLY,
				ResultSet.CONCUR_READ_ONLY)) {
			ps.setString(1, guid);
			ps.setString(2, versionId);
			try (final ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					final int lnkLuid = rs.getInt(1);
					final String lnkCntId = rs.getString(2);
					final String lnkName = rs.getString(3);
					final boolean lnkFolder = "Y".equals(rs.getString(4));
					final String objId = rs.getString(5);
					final String vrId = rs.getString(6);
					final String objTitle = rs.getString(7);
					final long objCreated = rs.getTimestamp(8).getTime();
					final long objDate = rs.getTimestamp(9).getTime();
					final String objOwner = rs.getString(10);
					final String objType = rs.getString(11);
					final int objState = rs.getInt(12);
					final boolean trListable = objState == MatLink.STATE_SYSTEM || objState == MatLink.STATE_PUBLISH;
					final boolean trSearchable = objState == MatLink.STATE_ARCHIVE || objState == MatLink.STATE_PUBLISH;
					final String extLink = rs.getString(13);
					return new LinkData(
							guid,
							lnkLuid,
							lnkCntId,
							lnkName,
							lnkFolder,
							objId,
							vrId,
							objTitle,
							objCreated,
							objDate,
							objOwner,
							objType,
							objState,
							trListable,
							trSearchable,
							extLink);
				}
				return null;
			}
		}
	}

	static void move(final Connection conn, final String initialCntLnkId, final String initialKey, final String cntLnkId, final String key) throws Exception {

		if (cntLnkId.equals(initialCntLnkId)) {
			try (final PreparedStatement ps = conn.prepareStatement("UPDATE s3Tree SET lnkName=? WHERE cntLnkId=? AND lnkName=?")) {
				ps.setString(1, key);
				ps.setString(2, initialCntLnkId);
				ps.setString(3, initialKey);
				ps.execute();
			}
		} else {
			try (final PreparedStatement ps = conn.prepareStatement("UPDATE s3Tree SET cntLnkId=?, lnkName=? WHERE cntLnkId=? AND lnkName=? AND lnkId!=?")) {
				ps.setString(1, cntLnkId);
				ps.setString(2, key);
				ps.setString(3, initialCntLnkId);
				ps.setString(4, initialKey);
				ps.setString(5, "$$ROOT_ENTRY");
				ps.execute();
			}
		}
	}

	static final void recycle(final Connection conn, final String lnkId) throws Exception {

		try (final PreparedStatement ps = conn
				.prepareStatement("INSERT INTO s3Recycled(delRootId,delDate,delObjId,delCntId,delOwner) SELECT lnkId,?,objId,cntLnkId,? FROM s3Tree WHERE lnkId=?")) {
			ps.setTimestamp(1, new Timestamp(Engine.fastTime()));
			ps.setString(2, Context.getUserId(Exec.currentProcess()));
			ps.setString(3, lnkId);
			ps.execute();
		}
		try (final PreparedStatement ps = conn.prepareStatement(
				"INSERT INTO s3RecycledTree(lnkId,delId,cntLnkId,lnkName,lnkFolder,objId) SELECT lnkId,lnkId,cntLnkId,lnkName,lnkFolder,objId FROM s3Tree WHERE lnkId=?")) {
			ps.setString(1, lnkId);
			ps.execute();
		}
		try (final PreparedStatement ps = conn.prepareStatement("UPDATE s3Tree SET cntLnkId='*' WHERE lnkId=?")) {
			ps.setString(1, lnkId);
			ps.execute();
		}
	}

	static void rename(final Connection conn, final String cntLnkId, final String initialKey, final String key) throws Exception {

		try (final PreparedStatement ps = conn.prepareStatement("UPDATE s3Tree SET lnkName=? WHERE cntLnkId=? AND lnkName=?")) {
			ps.setString(1, key);
			ps.setString(2, cntLnkId);
			ps.setString(3, initialKey);
			ps.execute();
		}
	}

	static final Collection<LinkData> searchLinks(final Connection conn, final String objId, final boolean all) throws Exception {

		Set<LinkData> result = null;
		try (final PreparedStatement ps = conn.prepareStatement(
				all
					? "SELECT t.lnkId, t.lnkLuid, t.cntLnkId FROM s3Tree t,s3Objects o WHERE o.objId=? AND t.objId=o.objId"
					: "SELECT t.lnkId, t.lnkLuid, t.cntLnkId FROM s3Tree t,s3Objects o WHERE o.objId=? AND t.objId=o.objId AND o.objState!='0'",
				ResultSet.TYPE_FORWARD_ONLY,
				ResultSet.CONCUR_READ_ONLY)) {
			ps.setString(1, objId);
			try (final ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					if (result == null) {
						result = Create.tempSet();
					}
					final String lnkId = rs.getString(1);
					final int lnkLuid = rs.getInt(2);
					final String lnkCntId = rs.getString(3);
					final LinkData linkData = new LinkData(lnkId, lnkLuid, lnkCntId, null, false, objId, null, '?', -1L, -1L, null, null, 0, false, false, null);
					result.add(linkData);
				}
			}
		}
		if (result == null) {
			return null;
		}
		return result;
	}

	static final void serializeCreate(final Connection conn, final String cntLnkId, final String lnkId, final String lnkName, final boolean lnkFolder, final String objId)
			throws Exception {

		try (final PreparedStatement ps = conn.prepareStatement("INSERT INTO s3Tree(lnkId,cntLnkId,lnkName,lnkFolder,objId) VALUES (?,?,?,?,?)")) {
			Report.event("MAT-LINK", "CREATE-LINK", "lnkId=" + lnkId + ", cntLnkId=" + cntLnkId + ", lnkName=" + lnkName + ", lnkFolder=" + lnkFolder + ", objId=" + objId);
			ps.setString(1, lnkId);
			ps.setString(2, cntLnkId);
			ps.setString(3, lnkName);
			ps.setString(
					4,
					lnkFolder
						? "Y"
						: "N");
			ps.setString(5, objId);
			ps.execute();
		}
	}

	static final void unlink(final Connection conn, final int lnkLuid) throws Exception {

		try (final PreparedStatement ps = conn.prepareStatement("DELETE FROM s3Tree WHERE lnkLuid=?")) {
			ps.setInt(1, lnkLuid);
			ps.execute();
		}
	}

	static final void unlink(final Connection conn, final String lnkId) throws Exception {

		try (final PreparedStatement ps = conn.prepareStatement("DELETE FROM s3Tree WHERE lnkId=?")) {
			ps.setString(1, lnkId);
			ps.execute();
		}
	}

	static final void update(final Connection conn, final String lnkId, final boolean initialFolder, final boolean folder) throws Exception {

		final List<String> setPart = new ArrayList<>();
		if (initialFolder != folder) {
			setPart.add("lnkFolder=?");
		}
		if (!setPart.isEmpty()) {
			{
				try (final PreparedStatement ps = conn.prepareStatement("UPDATE s3Tree SET " + Text.join(setPart, ", ") + " WHERE lnkId=?")) {
					int index = 1;
					if (initialFolder != folder) {
						ps.setString(
								index++,
								folder
									? "Y"
									: "N");
					}
					ps.setString(index, lnkId);
					ps.execute();
				}
			}
		}
	}
}
