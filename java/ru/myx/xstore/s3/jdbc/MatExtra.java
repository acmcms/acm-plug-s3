/*
 * Created on 01.07.2004
 */
package ru.myx.xstore.s3.jdbc;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import ru.myx.ae3.binary.Transfer;
import ru.myx.ae3.binary.TransferBuffer;
import ru.myx.ae3.binary.TransferCollector;
import ru.myx.ae3.binary.TransferCopier;
import ru.myx.ae3.help.Text;
import ru.myx.ae3.mime.MimeType;
import ru.myx.ae3.report.Report;
import ru.myx.xstore.s3.concept.TargetExternal;

/** @author myx */
final class MatExtra {

	static final boolean contains(final Connection conn, final String recId) throws Exception {

		try (final PreparedStatement ps = conn.prepareStatement("SELECT count(*) FROM s3Extra WHERE recId=?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
			ps.setString(1, recId);
			try (final ResultSet rs = ps.executeQuery()) {
				return rs.next() && rs.getInt(1) > 0;
			}
		}
	}

	static final boolean materialize(final ServerJdbc server, final Connection conn, final String recId, final TargetExternal target) throws Exception {

		try (final PreparedStatement ps = conn
				.prepareStatement("SELECT recDate,recType,recBlob FROM s3Extra WHERE recId=?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
			ps.setString(1, recId);
			try (final ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					server.stsLoadExternal++;
					final long recDate = rs.getTimestamp(1).getTime();
					final String recType = rs.getString(2);
					final InputStream recBlob = rs.getBinaryStream(3);
					if (recType == null) {
						throw new NullPointerException("type is null!");
					}
					final int length = recType.length();
					if (length == 0) {
						throw new NullPointerException("type is empty!");
					}
					final char first = recType.charAt(0);
					if (first == 'g' && length > 4 && recType.charAt(4) == ';' && recType.startsWith("gzip;")) {
						server.stsLoadExternalUngzip++;
						final GZIPInputStream uncompressed = new GZIPInputStream(recBlob);
						target.accept(recId, recDate, recType.substring(5).trim(), uncompressed);
						return true;
					}
					if (first == 'p' && length > 5 && recType.charAt(5) == ';' && recType.startsWith("plain;")) {
						server.stsLoadExternalPlain++;
						target.accept(recId, recDate, recType.substring(6).trim(), recBlob);
						return true;
					}
					if (first == 't' && length > 3 && recType.charAt(3) == ';' && recType.startsWith("try;")) {
						server.stsLoadExternalTry++;
						final String type = recType.substring(4).trim();
						target.accept(recId, recDate, type, recBlob);
						if (MimeType.compressByContentTypeTry(type, true)) {
							server.updateExtra(recId);
						}
						return true;
					}
					server.stsLoadExternalUnknown++;
					target.accept(recId, recDate, recType, recBlob);
					server.updateExtra(recId);
					return true;
				}
				server.stsLoadExternalFailed++;
				return false;
			}
		}
	}

	static final void purge(final Connection conn, final String recId) throws Exception {

		if (recId == null) {
			return;
		}
		try {
			final String query = "DELETE FROM s3Extra WHERE recId=?";
			try (final PreparedStatement ps = conn.prepareStatement(query)) {
				ps.setString(1, recId);
				ps.execute();
			}
		} catch (final Throwable t) {
			Report.exception("S3/JDBC/MAT_EXTRA", "Exception while purging extra record to an object", t);
		}
	}

	static final void serialize(final Connection conn,
			final String recId,
			final String objId,
			final String fldId,
			final long recDate,
			final String type,
			final TransferCopier copier,
			final boolean preferInsert) throws Exception {

		{
			final String recType = type;
			final long recBufferLength = copier.length();
			if (recBufferLength > Integer.MAX_VALUE) {
				throw new RuntimeException("Bigger than maximum byte array size, size=" + recBufferLength + "!");
			}
			try (final PreparedStatement ps = conn.prepareStatement("INSERT INTO s3Extra(recId,recDate,recType,recBlob) VALUES (?,?,?,?) ")) {
				ps.setString(1, recId);
				ps.setTimestamp(2, new Timestamp(recDate));
				if (copier.length() >= 256 && MimeType.compressByContentTypeTry(type, true)) {
					final TransferCollector collector = Transfer.createCollector();
					final GZIPOutputStream zip = new GZIPOutputStream(collector.getOutputStream());
					Transfer.toStream(copier.nextCopy(), zip, true);
					final TransferBuffer buffer = collector.toBuffer();
					final int length = (int) buffer.remaining();
					if (length < recBufferLength) {
						ps.setString(3, "gzip;" + recType);
						ps.setBinaryStream(4, buffer.toInputStream(), length);
					} else {
						ps.setString(3, "plain;" + recType);
						ps.setBinaryStream(4, copier.nextInputStream(), (int) recBufferLength);
					}
				} else {
					ps.setString(3, "try;" + recType);
					ps.setBinaryStream(4, copier.nextInputStream(), (int) recBufferLength);
				}
				ps.execute();
			}
		}
		try {
			final String fldIdPrepared = Text.limitString(fldId, 32);
			if (preferInsert) {
				try {
					try (final PreparedStatement ps = conn.prepareStatement("INSERT INTO s3ExtraLink(recId,objId,fldId) VALUES (?,?,?) ")) {
						ps.setString(1, recId);
						ps.setString(2, objId);
						ps.setString(3, fldIdPrepared);
						ps.execute();
					}
				} catch (final SQLException e) {
					try (final PreparedStatement ps = conn.prepareStatement("UPDATE s3ExtraLink SET recId=? WHERE objId=? AND fldId=?")) {
						ps.setString(1, recId);
						ps.setString(2, objId);
						ps.setString(3, fldIdPrepared);
						if (ps.executeUpdate() != 1) {
							throw new RuntimeException("No insert and no update done!");
						}
					}
				}
			} else {
				try (final PreparedStatement ps = conn.prepareStatement("UPDATE s3ExtraLink SET recId=? WHERE objId=? AND fldId=?")) {
					ps.setString(1, recId);
					ps.setString(2, objId);
					ps.setString(3, fldIdPrepared);
					final int count = ps.executeUpdate();
					if (count == 1) {
						return;
					}
					if (count > 1) {
						throw new IllegalStateException("update count is more than one!!! Check indices and primary keys in s3ExtraLink table!");
					}
				}
				try (final PreparedStatement ps = conn.prepareStatement("INSERT INTO s3ExtraLink(recId,objId,fldId) VALUES (?,?,?) ")) {
					ps.setString(1, recId);
					ps.setString(2, objId);
					ps.setString(3, fldIdPrepared);
					ps.execute();
				}
			}
		} catch (final Throwable t) {
			Report.exception("S3/JDBC/MAT_EXTRA", "Exception while linking extra record to an object, recId=" + recId + ", objId=" + objId + ", fldId=" + fldId, t);
		}
	}

	static final void unlink(final Connection conn, final String recId, final String objId) throws Exception {

		if (recId == null && objId == null) {
			return;
		}
		try {
			final String query = "DELETE FROM s3ExtraLink WHERE " //
					+ (recId != null && objId != null
						? "recId=? AND objId=?"
						: recId == null
							? "objId=?"
							: "recId=?");
			try (final PreparedStatement ps = conn.prepareStatement(query)) {
				if (recId != null && objId != null) {
					ps.setString(1, recId);
					ps.setString(2, objId);
				} else {
					ps.setString(
							1, //
							recId == null
								? objId
								: recId //
					);
				}
				ps.execute();
			}
		} catch (final Throwable t) {
			Report.exception("S3/JDBC/MAT_EXTRA", "Exception while unlinking extra record to an object", t);
		}
	}

	static final void update(final Connection conn, final String recId, final long recDate, final String type, final TransferCopier copier) throws Exception {

		{
			final String recType = type;
			final long recBufferLength = copier.length();
			if (recBufferLength > Integer.MAX_VALUE) {
				throw new RuntimeException("Bigger than maximum byte array size, size=" + recBufferLength + "!");
			}
			try (final PreparedStatement ps = conn.prepareStatement("UPDATE s3Extra SET recDate=?,recType=?,recBlob=? WHERE recId=?")) {
				ps.setTimestamp(1, new Timestamp(recDate));
				if (copier.length() >= 256 && MimeType.compressByContentTypeTry(type, true)) {
					final TransferCollector collector = Transfer.createCollector();
					final GZIPOutputStream zip = new GZIPOutputStream(collector.getOutputStream());
					Transfer.toStream(copier.nextCopy(), zip, true);
					final TransferBuffer buffer = collector.toBuffer();
					final int length = (int) buffer.remaining();
					if (length < recBufferLength) {
						ps.setString(2, "gzip;" + recType);
						ps.setBinaryStream(3, buffer.toInputStream(), length);
					} else {
						ps.setString(2, "plain;" + recType);
						ps.setBinaryStream(3, copier.nextInputStream(), (int) recBufferLength);
					}
				} else {
					ps.setString(2, "try;" + recType);
					ps.setBinaryStream(3, copier.nextInputStream(), (int) recBufferLength);
				}
				ps.setString(4, recId);
				ps.execute();
			}
		}
	}

	/** @param server
	 * @param conn
	 * @param recId
	 * @return true when item have been modified
	 * @throws Exception
	 */
	static final boolean update(final ServerJdbc server, final Connection conn, final String recId) throws Exception {

		final long newDate;
		final String newType;
		final TransferCopier newData;
		try (final PreparedStatement ps = conn
				.prepareStatement("SELECT recDate,recType,recBlob FROM s3Extra WHERE recId=?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
			ps.setString(1, recId);
			try (final ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					for (;;) {
						final long recDate = rs.getTimestamp(1).getTime();
						final String recType = rs.getString(2);
						final InputStream recBlob = rs.getBinaryStream(3);
						if (recType == null) {
							throw new NullPointerException("type is null!");
						}
						final int length = recType.length();
						if (length == 0) {
							throw new NullPointerException("type is empty!");
						}
						final char first = recType.charAt(0);
						if (first == 'g' && length > 4 && recType.charAt(4) == ';' && recType.startsWith("gzip;")) {
							server.stsLoadExternalUngzip++;
							final GZIPInputStream uncompressed = new GZIPInputStream(recBlob);
							final TransferCollector collector = Transfer.createCollector();
							Transfer.toStream(uncompressed, collector.getOutputStream(), true);
							newDate = recDate;
							newType = recType.substring(5).trim();
							newData = collector.toCloneFactory();
							break;
						}
						if (first == 'p' && length > 5 && recType.charAt(5) == ';' && recType.startsWith("plain;")) {
							server.stsLoadExternalPlain++;
							final TransferCollector collector = Transfer.createCollector();
							Transfer.toStream(recBlob, collector.getOutputStream(), true);
							newDate = recDate;
							newType = recType.substring(6).trim();
							newData = collector.toCloneFactory();
							break;
						}
						if (first == 't' && length > 3 && recType.charAt(3) == ';' && recType.startsWith("try;")) {
							server.stsLoadExternalTry++;
							final TransferCollector collector = Transfer.createCollector();
							Transfer.toStream(recBlob, collector.getOutputStream(), true);
							newDate = recDate;
							newType = recType.substring(4).trim();
							newData = collector.toCloneFactory();
							break;
						}
						server.stsLoadExternalUnknown++;
						final TransferCollector collector = Transfer.createCollector();
						Transfer.toStream(recBlob, collector.getOutputStream(), true);
						newDate = recDate;
						newType = recType;
						newData = collector.toCloneFactory();
						break;
					}
				} else {
					server.stsLoadExternalFailed++;
					return false;
				}
			}
		}
		MatExtra.update(conn, recId, newDate, newType, newData);
		server.stsExternalsUpdated++;
		return true;
	}

	private MatExtra() {

		// empty
	}

}
