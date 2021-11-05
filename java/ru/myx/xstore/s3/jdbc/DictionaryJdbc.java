/*
 * Created on 30.08.2004
 *
 * Window - Preferences - Java - Code Style - Code Templates
 */
package ru.myx.xstore.s3.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import ru.myx.ae1.storage.StorageImpl;
import ru.myx.ae2.indexing.IndexingDictionaryAbstract;
import ru.myx.ae3.help.Format;
import ru.myx.ae3.status.StatusFiller;
import ru.myx.ae3.status.StatusInfo;

/** @author myx
 *
 *         Window - Preferences - Java - Code Style - Code Templates */
final class DictionaryJdbc extends IndexingDictionaryAbstract implements StatusFiller {

	private final StorageImpl storage;

	private int stsGetPatternCodes = 0;

	private int stsGetWordCode = 0;

	private int stsGetWordCodeHits = 0;

	private int stsStoreWordCode = 0;

	private int stsStoreWordCodeHits = 0;

	DictionaryJdbc(final StorageImpl storage) {

		this.storage = storage;
	}

	@Override
	public final boolean areCodesUnique() {

		return true;
	}

	@Override
	public final int[] getPatternCodes(final String pattern, final boolean exact, final boolean required) {

		this.stsGetPatternCodes++;
		try (final Connection conn = this.storage.nextConnection()) {
			try (final PreparedStatement ps = conn
					.prepareStatement("SELECT code FROM s3Dictionary WHERE word LIKE ? AND exact=?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
				ps.setString(1, pattern.replace('*', '%').replace('?', '_'));
				ps.setString(
						2,
						exact
							? "Y"
							: "N");
				try (final ResultSet rs = ps.executeQuery()) {
					if (rs.next()) {
						final List<Integer> result = new ArrayList<>();
						do {
							result.add(Integer.valueOf(rs.getInt(1)));
						} while (rs.next());
						final int size = result.size();
						if (size == 1) {
							return new int[]{
									result.get(0).intValue()
							};
						}
						final int[] array = new int[size];
						for (int i = size - 1; i >= 0; --i) {
							array[i] = result.get(i).intValue();
						}
						return array;
					}
					return required
						? new int[]{
								-1
						}
						: null;
				}
			}
		} catch (final SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public final int getWordCode(final String word, final boolean exact, final boolean required) {

		this.stsGetWordCode++;
		try (final Connection conn = this.storage.nextConnection()) {
			final PreparedStatement ps = conn.prepareStatement("SELECT code FROM s3Dictionary WHERE word=? AND exact=?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			try {
				ps.setString(1, word);
				ps.setString(
						2,
						exact
							? "Y"
							: "N");
				try (final ResultSet rs = ps.executeQuery()) {
					if (rs.next()) {
						this.stsGetWordCodeHits++;
						return rs.getInt(1);
					}
					return required
						? -1
						: 0;
				}
			} finally {
				try {
					ps.close();
				} catch (final Throwable t) {
					// ignore
				}
			}
		} catch (final SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void statusFill(final StatusInfo status) {

		status.put("NET-DICT, getPatternCodes", Format.Compact.toDecimal(this.stsGetPatternCodes));
		status.put("NET-DICT, getWordCode", Format.Compact.toDecimal(this.stsGetWordCode));
		status.put("NET-DICT, getWordCode hits", Format.Compact.toDecimal(this.stsGetWordCodeHits));
		status.put("NET-DICT, storeWordCode", Format.Compact.toDecimal(this.stsStoreWordCode));
		status.put("NET-DICT, storeWordCode hits", Format.Compact.toDecimal(this.stsStoreWordCodeHits));
	}

	@Override
	public final int storeWordCode(final String word, final boolean exact, final Object attachment) {

		this.stsStoreWordCode++;
		try {
			if (attachment != null && attachment instanceof Connection) {
				return this.storeWordCodeImpl(word, exact, (Connection) attachment);
			}
			try (final Connection conn = this.storage.nextConnection()) {
				return this.storeWordCodeImpl(word, exact, conn);
			}
		} catch (final SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private final int storeWordCodeImpl(final String word, final boolean exact, final Connection conn) throws SQLException {

		try (final PreparedStatement ps = conn
				.prepareStatement("SELECT code FROM s3Dictionary WHERE word=? AND exact=?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
			ps.setString(1, word);
			ps.setString(
					2,
					exact
						? "Y"
						: "N");
			try (final ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					this.stsStoreWordCodeHits++;
					return rs.getInt(1);
				}
			}
		}
		try (final PreparedStatement ps = conn.prepareStatement("INSERT INTO s3Dictionary(word,exact) VALUES (?,?)")) {
			ps.setString(1, word);
			ps.setString(
					2,
					exact
						? "Y"
						: "N");
			ps.execute();
		}
		{
			try (final PreparedStatement ps = conn
					.prepareStatement("SELECT code FROM s3Dictionary WHERE word=? AND exact=?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
				ps.setString(1, word);
				ps.setString(
						2,
						exact
							? "Y"
							: "N");
				try (final ResultSet rs = ps.executeQuery()) {
					if (rs.next()) {
						return rs.getInt(1);
					}
					return 0;
				}
			}
		}
	}

}
