/**
 *
 */
package ru.myx.xstore.s3.local;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.lang.ref.Reference;
import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;
import java.util.ConcurrentModificationException;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;

import ru.myx.ae1.storage.ModuleInterface;
import ru.myx.ae3.Engine;
import ru.myx.ae3.act.Act;
import ru.myx.ae3.binary.Transfer;
import ru.myx.ae3.binary.TransferCopier;
import ru.myx.ae3.extra.External;
import ru.myx.ae3.extra.ExtraBinary;
import ru.myx.ae3.extra.ExtraBytes;
import ru.myx.ae3.extra.ExtraSerialized;
import ru.myx.ae3.extra.ExtraTextBase;
import ru.myx.ae3.extra.ExtraXml;
import ru.myx.ae3.help.Format;
import ru.myx.ae3.mime.MimeType;
import ru.myx.ae3.mime.MimeTypeCompressibility;
import ru.myx.ae3.report.Report;
import ru.myx.io.DataInputBufferedReusable;
import ru.myx.io.DataOutputBufferedReusable;
import ru.myx.xstore.s3.StorageLevel3;
import ru.myx.xstore.s3.concept.InvalidationEventType;
import ru.myx.xstore.s3.concept.LinkData;
import ru.myx.xstore.s3.concept.StorageNetwork;
import ru.myx.xstore.s3.concept.TreeData;

/** @author myx */
final class Leaf {
	
	private static final int RT_INCOMPLETE = 0x02206241;
	
	private static final int RT_EXTRA = 0x00000303;
	
	private static final int RT_MAP = 0x00070606;
	
	private static final long CACHE_EXTRA = 1L * 60_000L * 60L * 24L * 7L;
	
	private static final long CACHE_TTL = 2L * 60_000L * 60L * 24L * 7L;
	
	private static final TreeData EMPTY_TREE = TreeData.EMPTY_TREE;
	
	private static final Reference<External> extraLoading = new WeakReference<>(null);
	
	private static final Reference<External> extraUndefined = new WeakReference<>(null);
	
	private static final int FOLDERS_NAME_RADIX = 32;
	
	private static final Reference<LinkData> linkLoading = new WeakReference<>(null);
	
	private static final Reference<LinkData> linkUndefined = new WeakReference<>(null);
	
	private static final LinkData[] listLoading = new LinkData[0];
	
	private static final LinkData[] listUndefined = new LinkData[0];
	
	private static final String OWNER = "S3-LCLEAF";
	
	private static final int STATE_ARCHIVE = ModuleInterface.STATE_ARCHIVE;
	
	private static final int STATE_PUBLISH = ModuleInterface.STATE_PUBLISH;
	
	private static final int STATE_SYSTEM = ModuleInterface.STATE_SYSTEM;
	
	private static final Reference<TreeData> treeLoading = new WeakReference<>(null);
	
	private static final Reference<TreeData> treeUndefined = new WeakReference<>(null);
	
	private static final MimeType MT_TEXT_XML = MimeType.getMimeType("text/xml", MimeTypeCompressibility.SURE_COMPRESS, true);
	
	private static final MimeType MT_AE3_STRING = MimeType.getMimeType("application/vnd.myx.ae3-string", MimeTypeCompressibility.SURE_COMPRESS, true);
	
	private static final MimeType MT_AE3_MAPXML = MimeType.getMimeType("application/vnd.myx.ae3-mapxml", MimeTypeCompressibility.SURE_COMPRESS, true);
	
	private static final MimeType MT_AE3_BUFFER = MimeType.getMimeType("application/vnd.myx.ae3-buffer", MimeTypeCompressibility.TRY_COMPRESS, true);
	
	private static final MimeType MT_AE3_COPIER = MimeType.getMimeType("application/vnd.myx.ae3-copier", MimeTypeCompressibility.TRY_COMPRESS, true);
	
	private static final void cleanRecursive(final File folder) {
		
		final File[] children = folder.listFiles();
		if (children != null && children.length > 0) {
			for (int i = children.length - 1; i >= 0; --i) {
				final File child = children[i];
				if (child.isDirectory()) {
					Leaf.cleanRecursive(child);
				} else {
					child.delete();
				}
			}
		}
		folder.delete();
	}
	
	private static final LinkData readLinkData(final String identifier, final DataInput din) throws Exception {
		
		final int lnkLuid = din.readInt();
		final String lnkCntId = din.readUTF();
		final String lnkName = din.readUTF();
		final boolean lnkFolder = din.readBoolean();
		final String objId = din.readUTF();
		final String vrId = din.readUTF();
		final String objTitle = din.readUTF();
		final long objCreated = din.readLong();
		final long objModified = din.readLong();
		final String objOwner = din.readUTF();
		final String objType = din.readUTF();
		final int objState = din.readInt();
		final boolean trListable = objState == Leaf.STATE_SYSTEM || objState == Leaf.STATE_PUBLISH;
		final boolean trSearchable = objState == Leaf.STATE_ARCHIVE || objState == Leaf.STATE_PUBLISH;
		final String extLink = din.readUTF();
		return new LinkData(
				identifier,
				lnkLuid,
				lnkCntId,
				lnkName,
				lnkFolder,
				objId,
				vrId,
				objTitle,
				objCreated,
				objModified,
				objOwner,
				objType,
				objState,
				trListable,
				trSearchable,
				extLink);
	}
	
	private static final LinkData[] readListing(final DataInput din) throws Exception {
		
		final int length = din.readInt();
		if (length == 0) {
			return TreeData.ENTRY_ARRAY_EMPTY;
		}
		final LinkData[] entries = new LinkData[length];
		for (int i = 0; i < length; ++i) {
			final LinkData entry = Leaf.readListingLinkData(din);
			entries[i] = entry;
		}
		return entries;
	}
	
	private static final LinkData readListingLinkData(final DataInput din) throws Exception {
		
		final String lnkId = din.readUTF();
		final boolean lnkFolder = din.readBoolean();
		final long objCreated = din.readLong();
		final long objModified = din.readLong();
		final int objState = din.readInt();
		final boolean trListable = objState == Leaf.STATE_SYSTEM || objState == Leaf.STATE_PUBLISH;
		final boolean trSearchable = objState == Leaf.STATE_ARCHIVE || objState == Leaf.STATE_PUBLISH;
		return new LinkData(lnkId, -1, null, null, lnkFolder, null, null, '?', objCreated, objModified, null, null, objState, trListable, trSearchable, null);
	}
	
	private static final TreeData readTreeData(final String identifier, final DataInput din) throws Exception {
		
		final int length = din.readInt();
		if (length == 0) {
			return Leaf.EMPTY_TREE;
		}
		final LinkData[] entries = new LinkData[length];
		boolean allFiles = true;
		boolean allFolders = true;
		for (int i = 0; i < length; ++i) {
			final LinkData entry = Leaf.readTreeLinkData(din);
			allFiles &= !entry.lnkFolder;
			allFolders &= entry.lnkFolder;
			entries[i] = entry;
		}
		return new TreeData(identifier, entries, allFiles, allFolders);
	}
	
	private static final LinkData readTreeLinkData(final DataInput din) throws Exception {
		
		final String lnkId = din.readUTF();
		final String lnkName = din.readUTF();
		final boolean lnkFolder = din.readBoolean();
		final char objTitleLetter = din.readChar();
		final long objCreated = din.readLong();
		final long objModified = din.readLong();
		final String objType = din.readUTF();
		final int objState = din.readInt();
		final boolean trListable = objState == Leaf.STATE_SYSTEM || objState == Leaf.STATE_PUBLISH;
		final boolean trSearchable = objState == Leaf.STATE_ARCHIVE || objState == Leaf.STATE_PUBLISH;
		return new LinkData(lnkId, -1, null, lnkName, lnkFolder, null, null, objTitleLetter, objCreated, objModified, null, objType, objState, trListable, trSearchable, null);
	}
	
	private static final void writeExpiration(final DataOutput dout, final long ttl) throws Exception {
		
		dout.writeLong(Engine.fastTime() + Math.round(ttl - ThreadLocalRandom.current().nextDouble() * ttl * 0.1));
	}
	
	private final Map<String, CacheEntry> cache;
	
	private final DataInputBufferedReusable dataIn;
	
	private final DataOutputBufferedReusable dataOut;
	
	private final File[] folders;
	
	private final Object issuer;
	
	private final StorageNetwork parent;
	
	private final ServerLocal server;
	
	private final StorageLevel3 storage;
	
	private int stsCheckExpired;
	
	private int stsCheckFile;
	
	private int stsCheckRequests;
	
	private int stsReadExternal;
	
	private int stsReadLink;
	
	private int stsReadLinkExpired;
	
	private int stsReadSearchIdentity;
	
	private int stsReadSearchIdentityExpired;
	
	private int stsReadSearchLinks;
	
	private int stsReadSearchLinksExpired;
	
	private int stsReadSearchLocal;
	
	private int stsReadSearchLocalExpired;
	
	private int stsReadTree;
	
	private int stsReadTreeExpired;
	
	private int stsWriteExternal;
	
	private int stsWriteLink;
	
	private int stsWriteSearchIdentity;
	
	private int stsWriteSearchLinks;
	
	private int stsWriteSearchLocal;
	
	private int stsWriteTree;
	
	Leaf(
			final DataInputBufferedReusable dataIn,
			final DataOutputBufferedReusable dataOut,
			final StorageLevel3 storage,
			final ServerLocal server,
			final StorageNetwork parent,
			final Object issuer,
			final File[] folders) {
		
		this.storage = storage;
		this.server = server;
		this.parent = parent;
		this.issuer = issuer;
		this.cache = new TreeMap<>();
		this.dataIn = dataIn;
		this.dataOut = dataOut;
		this.folders = folders;
	}
	
	private final File getCheckFolderObject(final int index) {
		
		final File folder = this.folders[index];
		if (folder != null) {
			return folder;
		}
		final File current = new File(
				this.server.getFolderLocal(),
				index < Leaf.FOLDERS_NAME_RADIX
					? "0" + Integer.toString(index, Leaf.FOLDERS_NAME_RADIX)
					: Integer.toString(index, Leaf.FOLDERS_NAME_RADIX));
		if (!current.isDirectory()) {
			current.delete();
		}
		this.folders[index] = current;
		return current;
	}
	
	/** @param identifier */
	private void invalidateCacheEntry(final String identifier, final boolean delete) {
		
		final CacheEntry entry = this.cache.get(identifier);
		if (entry != null) {
			{
				final Reference<LinkData> reference = entry.linkData;
				if (reference != null && reference != Leaf.linkLoading) {
					final LinkData data = reference.get();
					if (data != null) {
						data.invalid = true;
					}
					entry.linkData = null;
					reference.clear();
				}
			}
			{
				final Reference<TreeData> reference = entry.treeData;
				if (reference != null && reference != Leaf.treeLoading) {
					final TreeData data = reference.get();
					if (data != null) {
						data.invalid = true;
					}
					entry.treeData = null;
					reference.clear();
				}
			}
			{
				final Reference<External> reference = entry.extraData;
				if (reference != null && reference != Leaf.extraLoading) {
					entry.extraData = null;
					reference.clear();
				}
			}
			{
				final Reference<Map<String, LinkData[]>> arrays = entry.arrays;
				if (arrays != null) {
					arrays.clear();
					entry.arrays = null;
				}
			}
			if (delete) {
				this.cache.remove(identifier);
			}
		}
	}
	
	private final External load0x0303(final String identifier, final File serialized, final DataInput din) throws IOException {
		
		din.readLong(); // expires
		din.readLong(); // updated
		final long date = din.readLong();
		final String type = din.readUTF();
		final int skip = din.readInt();
		final int length = (int) serialized.length();
		final TransferCopier copier;
		if (length - skip <= 4096) {
			final byte[] buffer = new byte[length - skip];
			din.readFully(buffer);
			copier = Transfer.wrapCopier(buffer);
		} else {
			copier = Transfer.createCopier(serialized, skip, serialized.length());
		}
		switch (type.length()) {
			case 8 : {
				if ("text/xml".equals(type)) {
					return new ExtraXml(this.issuer, identifier, date, copier, this.server.getStorageExternalizerReference(), null);
				}
				break;
			}
			case 9 : {
				if ("ae2/bytes".equals(type)) {
					return new ExtraBytes(this.issuer, identifier, date, copier);
				}
				break;
			}
			case 10 : {
				final char first = type.charAt(0);
				if (first == 'a' && "ae2/binary".equals(type)) {
					return new ExtraBinary(this.issuer, identifier, date, copier);
				}
				if (first == 't' && "text/plain".equals(type)) {
					return new ExtraTextBase(this.issuer, identifier, date, copier);
				}
				break;
			}
			default :
		}
		return new ExtraSerialized(this.issuer, identifier, date, type, copier);
	}
	
	private final RandomAccessFile openForUpdate(final File serialized) throws Exception {
		
		try {
			return new RandomAccessFile(serialized, "rw");
		} catch (final FileNotFoundException e) {
			if (e.getMessage().indexOf("orrupt") != -1 || e.getMessage().indexOf("eadable") != -1) {
				Report.exception(Leaf.OWNER, "trying to recover from filesystem problem!", e);
				this.cache.clear();
				try {
					Thread.sleep(500L);
					final File leafFile = serialized.getParentFile();
					if (!leafFile.delete()) {
						Thread.sleep(1000L);
						if (!leafFile.delete()) {
							final File cacheFile = leafFile.getParentFile();
							final File cacheRoot = cacheFile.getParentFile();
							final File corruptedRoot = new File(cacheRoot, "corrupted");
							corruptedRoot.mkdirs();
							final File corruptedFile = new File(corruptedRoot, Format.Compact.date(System.currentTimeMillis()) + "-" + serialized.getName());
							if (!leafFile.renameTo(corruptedFile)) {
								Thread.sleep(1500L);
								if (!leafFile.renameTo(corruptedFile)) {
									Report.exception(Leaf.OWNER, "Cannot get rid of corrupted folder!", e);
								}
							}
						}
					}
					leafFile.mkdirs();
					this.cache.clear();
				} catch (final InterruptedException e1) {
					// ignore
				}
				return new RandomAccessFile(serialized, "rw");
			}
			Report.warning(Leaf.OWNER, "File is busy for writing, delay 150 ms, filename=" + serialized.getAbsolutePath());
			Thread.sleep(150L);
			try {
				return new RandomAccessFile(serialized, "rw");
			} catch (final FileNotFoundException e2) {
				Report.warning(Leaf.OWNER, "File is busy for writing, delay 500 ms, filename=" + serialized.getAbsolutePath());
				Thread.sleep(500L);
				return new RandomAccessFile(serialized, "rw");
			}
		}
	}
	
	final int accept(final int folderIndex, final String identifier, final long date, final String type, final InputStream data) throws Exception {
		
		synchronized (this.dataIn) {
			final File folder = this.getCheckFolderObject(folderIndex);
			final File target = new File(folder, identifier);
			folder.mkdirs();
			if (target.isDirectory()) {
				Leaf.cleanRecursive(target);
			}
			this.stsWriteExternal++;
			try (final RandomAccessFile out = this.openForUpdate(target)) {
				this.dataOut.setOutput(out);
				this.dataOut.writeInt(Leaf.RT_INCOMPLETE);
				Leaf.writeExpiration(this.dataOut, Leaf.CACHE_EXTRA);
				this.dataOut.writeLong(Engine.fastTime());
				this.dataOut.writeLong(date);
				this.dataOut.writeUTF(type);
				this.dataOut.writeInt(this.dataOut.getWrittenByteCount() + 4);
				final int written = this.dataOut.writeFullyFromStream(data);
				this.dataOut.close();
				out.setLength(out.getFilePointer());
				out.seek(0L);
				out.writeInt(Leaf.RT_EXTRA);
				return written;
			} finally {
				data.close();
			}
		}
	}
	
	final void check(final int folderIndex) throws Exception {
		
		final File folder = this.getCheckFolderObject(folderIndex);
		final File[] extras = folder.listFiles();
		if (extras == null) {
			return;
		}
		final DataInputBufferedReusable din = this.dataIn;
		final Map<String, File> check = new TreeMap<>();
		for (int i = extras.length - 1; i >= 0; --i) {
			final File extra = extras[i];
			if (extra.isDirectory()) {
				Leaf.cleanRecursive(extra);
				continue;
			}
			final long startDate = System.currentTimeMillis();
			final long presenceValid = this.storage.getPresenceValidityDate();
			int version = 0;
			long expires = -1L;
			long updated = -1L;
			long created = -1L;
			synchronized (din) {
				try {
					din.setStream(new FileInputStream(extra));
				} catch (final FileNotFoundException e) {
					Report.info(Leaf.OWNER, "Extra deleted while checking: guid=" + extra.getName());
					continue;
				}
				this.stsCheckFile++;
				try {
					version = din.readInt();
					switch (version) {
						case RT_EXTRA : {
							expires = din.readLong();
							if (expires > Engine.fastTime()) {
								continue;
							}
							this.stsCheckExpired++;
							check.put(extra.getName(), extra);
							updated = din.readLong();
							created = din.readLong();
							continue;
						}
						case RT_MAP : {
							expires = din.readLong();
							updated = din.readLong();
							created = din.readLong();
							if (expires > Engine.fastTime() && created > presenceValid) {
								continue;
							}
							break;
						}
						default :
					}
				} catch (final IOException e) {
					// ignore - remove
				} finally {
					din.close();
				}
			}
			if (created == -1L) {
				Report.info(
						Leaf.OWNER,
						"Extra stale (" + Integer.toHexString(version) + "): guid=" + extra.getName() + ", size=" + Format.Compact.toBytes(extra.length()) + ", took="
								+ Format.Compact.toPeriod(System.currentTimeMillis() - startDate));
			} else {
				Report.info(
						Leaf.OWNER,
						"Extra stale (" + Integer.toHexString(version) + "): guid=" + extra.getName() + ", size=" + Format.Compact.toBytes(extra.length()) + ", life="
								+ Format.Compact.toPeriod(Engine.fastTime() - created) + ", chng=" + (updated <= 0
									? "never"
									: Format.Compact.toPeriod(Engine.fastTime() - updated))
								+ ", dead=" + Format.Compact.toPeriod(Engine.fastTime() - expires) + ", took=" + Format.Compact.toPeriod(System.currentTimeMillis() - startDate));
			}
			synchronized (din) {
				if (extra.delete()) {
					continue;
				}
			}
			Report.info(
					Leaf.OWNER,
					"Extra clean fail - will retry: guid=" + extra.getName() + ", life=" + Format.Compact.toPeriod(System.currentTimeMillis() - extra.lastModified()) + ", size="
							+ Format.Compact.toBytes(extra.length()));
			Act.later(null, new InvalidatorRetry(extra), 500L);
		}
		if (check.isEmpty()) {
			return;
		}
		this.stsCheckRequests++;
		final Set<String> existing = this.parent.hasExternals(check.keySet());
		if (existing != null && !existing.isEmpty()) {
			for (final String guid : existing) {
				final File extra = check.remove(guid);
				synchronized (din) {
					try (final RandomAccessFile dio = this.openForUpdate(extra)) {
						dio.seek(4);
						Leaf.writeExpiration(dio, Leaf.CACHE_EXTRA);
						dio.writeLong(Engine.fastTime());
					}
				}
				if (Report.MODE_DEBUG) {
					Report.info(Leaf.OWNER, "Extra re-validated, guid=" + guid + ", size=" + Format.Compact.toBytes(extra.length()));
				}
			}
		}
		for (final Map.Entry<String, File> current : check.entrySet()) {
			final String guid = current.getKey();
			final File extra = current.getValue();
			if (Report.MODE_DEBUG || Report.MODE_ASSERT) {
				Report.info(Leaf.OWNER, "Extra stale (0303): guid=" + extra.getName() + ", size=" + Format.Compact.toBytes(extra.length()));
			}
			synchronized (din) {
				if (extra.delete()) {
					continue;
				}
			}
			if (extra.exists()) {
				Report.info(
						Leaf.OWNER,
						"Extra clean fail - will retry: guid=" + guid + ", life=" + Format.Compact.toPeriod(System.currentTimeMillis() - extra.lastModified()) + ", size="
								+ Format.Compact.toBytes(extra.length()));
				Act.later(null, new InvalidatorRetry(extra), 500L);
			}
		}
	}
	
	final External getExternal(final int folderIndex, final Object attachment, final String identifier) {
		
		try {
			final CacheEntry cache = this.cache.get(identifier);
			if (cache != null) {
				if (cache.extraData != null) {
					if (cache.extraData == Leaf.extraLoading) {
						synchronized (cache) {
							while (cache.extraData == Leaf.extraLoading) {
								try {
									cache.wait(0L);
								} catch (final InterruptedException e) {
									throw new RuntimeException(e);
								}
							}
						}
					}
					if (cache.extraData == Leaf.extraUndefined) {
						return null;
					}
					final External ready = cache.extraData.get();
					if (ready != null) {
						return ready;
					}
				}
			}
		} catch (final ConcurrentModificationException e) {
			Report.info(Leaf.OWNER, "Extra concurrent modification: guid=" + identifier);
		}
		boolean load = false;
		CacheEntry cache = null;
		try {
			final File serialized;
			External result = null;
			final DataInputBufferedReusable din = this.dataIn;
			synchronized (din) {
				cache = this.cache.get(identifier);
				if (cache != null) {
					if (cache.extraData == Leaf.extraUndefined) {
						return null;
					}
					if (cache.extraData != Leaf.extraLoading) {
						if (cache.extraData == null) {
							cache.extraData = Leaf.extraLoading;
							load = true;
						} else {
							result = cache.extraData.get();
							if (result != null) {
								return result;
							}
							cache.extraData = Leaf.extraLoading;
							load = true;
						}
					}
				} else {
					cache = new CacheEntry();
					cache.extraData = Leaf.extraLoading;
					this.cache.put(identifier, cache);
					load = true;
				}
				final File folder = this.getCheckFolderObject(folderIndex);
				serialized = new File(folder, identifier);
				if (load) {
					this.stsReadExternal++;
					try {
						din.setStream(new FileInputStream(serialized));
						try {
							final int version = din.readInt();
							if (version == Leaf.RT_EXTRA) {
								result = this.load0x0303(identifier, serialized, din);
								cache.extraData = new SoftReference<>(result);
							}
						} finally {
							din.close();
						}
					} catch (final IOException e) {
						if (serialized.exists()) {
							if (serialized.isDirectory()) {
								Leaf.cleanRecursive(serialized);
							} else {
								throw e;
							}
						}
					}
				}
			}
			if (!load) {
				if (cache.extraData == Leaf.extraUndefined) {
					return null;
				}
				if (cache.extraData == Leaf.extraLoading) {
					synchronized (cache) {
						while (cache.extraData == Leaf.extraLoading) {
							try {
								cache.wait(0L);
							} catch (final InterruptedException e) {
								throw new RuntimeException(e);
							}
						}
					}
				}
				return cache.extraData.get();
			}
			if (result == null) {
				final long startDate = System.currentTimeMillis();
				final boolean loaded = this.parent.loadExternal(attachment, identifier, this.server);
				if (loaded) {
					Report.info(
							Leaf.OWNER,
							"Extra imported: guid=" + identifier + ", size=" + Format.Compact.toBytes(serialized.length()) + ", took="
									+ Format.Compact.toPeriod(System.currentTimeMillis() - startDate));
					if (serialized.exists()) {
						synchronized (din) {
							din.setStream(new FileInputStream(serialized));
							try {
								final int version = din.readInt();
								if (version == Leaf.RT_EXTRA) {
									result = this.load0x0303(identifier, serialized, din);
								}
							} finally {
								din.close();
							}
							
						}
					}
				}
			}
			if (cache.extraData == Leaf.extraLoading) {
				cache.extraData = result == null
					? Leaf.extraUndefined
					: new SoftReference<>(result);
			}
			return result;
		} catch (final Error e) {
			if (load && cache != null) {
				if (cache.extraData == Leaf.extraLoading) {
					cache.extraData = Leaf.extraUndefined;
				}
				this.server.enqueueDiscard(identifier, InvalidationEventType.DEXTR);
			}
			throw e;
		} catch (final RuntimeException e) {
			if (load && cache != null) {
				if (cache.extraData == Leaf.extraLoading) {
					cache.extraData = Leaf.extraUndefined;
				}
				this.server.enqueueDiscard(identifier, InvalidationEventType.DEXTR);
			}
			throw e;
		} catch (final Throwable e) {
			if (load && cache != null) {
				if (cache.extraData == Leaf.extraLoading) {
					cache.extraData = Leaf.extraUndefined;
				}
				this.server.enqueueDiscard(identifier, InvalidationEventType.DEXTR);
			}
			throw new RuntimeException(e);
		} finally {
			if (load && cache != null) {
				synchronized (cache) {
					cache.notifyAll();
				}
			}
		}
	}
	
	final Object getIssuer() {
		
		return this.issuer;
	}
	
	final LinkData getLinkData(final int folderIndex, final String identifier) {
		
		try {
			final CacheEntry cache = this.cache.get(identifier);
			if (cache != null) {
				if (cache.linkData != null) {
					if (cache.linkData == Leaf.linkLoading) {
						synchronized (cache) {
							while (cache.linkData == Leaf.linkLoading) {
								try {
									cache.wait(0L);
								} catch (final InterruptedException e) {
									throw new RuntimeException(e);
								}
							}
						}
					}
					final Reference<LinkData> reference = cache.linkData;
					if (reference != null) {
						if (reference == Leaf.linkUndefined) {
							return null;
						}
						final LinkData ready = reference.get();
						if (ready != null && !ready.invalid) {
							return ready;
						}
					}
				}
			}
		} catch (final ConcurrentModificationException e) {
			Report.info(Leaf.OWNER, "Extra concurrent modification: guid=" + identifier);
		}
		
		final long startDate = System.currentTimeMillis();
		boolean load = false;
		CacheEntry cache = null;
		try {
			final File folder;
			final File serialized;
			LinkData result = null;
			boolean rewrite = false;
			boolean expired = false;
			synchronized (this.dataIn) {
				cache = this.cache.get(identifier);
				if (cache != null) {
					if (cache.linkData == Leaf.linkUndefined) {
						return null;
					}
					if (cache.linkData != Leaf.linkLoading) {
						if (cache.linkData == null) {
							cache.linkData = Leaf.linkLoading;
							load = true;
						} else {
							result = cache.linkData.get();
							if (result != null && !result.invalid) {
								return result;
							}
							cache.linkData = Leaf.linkLoading;
							load = true;
						}
					}
				} else {
					cache = new CacheEntry();
					cache.linkData = Leaf.linkLoading;
					this.cache.put(identifier, cache);
					load = true;
				}
				folder = this.getCheckFolderObject(folderIndex);
				serialized = new File(folder, identifier);
				if (load) {
					this.stsReadLink++;
					try {
						final DataInputBufferedReusable din = this.dataIn;
						din.setStream(new FileInputStream(serialized));
						try {
							final long length = serialized.length();
							if (length < 1024) {
								din.setReadLimit(1024);
								final int version = din.readInt(); // version
								if (version == Leaf.RT_MAP) {
									if (din.readLong() < Engine.fastTime()) { // expires
										expired = true;
									}
									din.readLong(); // updated
									if (din.readLong() < this.storage.getPresenceValidityDate()) { // created
										expired = true;
										rewrite = true;
									} else {
										while (din.available() > 0) {
											final String key = din.readUTF();
											final int size = din.readInt();
											if (size <= 0) {
												rewrite = true;
												break;
											}
											if (key.length() >= 4 && key.charAt(0) == '\n' && key.charAt(1) == '$') {
												final char c1 = key.charAt(2);
												final char c2 = key.charAt(3);
												if (c1 == 'S' && c2 == 'I') {
													synchronized (cache) {
														cache.setByKey(key, Leaf.readListing(din));
													}
													continue;
												}
												if (c1 == 'S' && c2 == 'i') {
													synchronized (cache) {
														cache.setByKey(key, Leaf.readListing(din));
													}
													continue;
												}
												if (c1 == 'S' && c2 == 'L') {
													synchronized (cache) {
														cache.setByKey(key, Leaf.readListing(din));
													}
													continue;
												}
												if (c1 == 's' && c2 == 'l') {
													synchronized (cache) {
														cache.setByKey(key, Leaf.readListing(din));
													}
													continue;
												}
												if (c1 == 'T' && c2 == 'R' && (cache.treeData == null || cache.treeData.get() == null)) {
													cache.treeData = new SoftReference<>(Leaf.readTreeData(identifier, din));
													continue;
												}
												if (c1 == 'I' && c2 == 't') {
													result = Leaf.readLinkData(identifier, din);
													cache.linkData = new SoftReference<>(result);
													continue;
												}
											}
											din.skipBytes(size);
										}
									}
								} else {
									rewrite = true;
								}
							} else {
								final int version = din.readInt(); // version
								if (version == Leaf.RT_MAP) {
									if (din.readLong() < Engine.fastTime()) { // expires
										expired = true;
									}
									din.readLong(); // updated
									if (din.readLong() < this.storage.getPresenceValidityDate()) { // created
										expired = true;
										rewrite = true;
									} else {
										while (din.available() > 0) {
											final String key = din.readUTF();
											final int size = din.readInt();
											if (size <= 0) {
												rewrite = true;
												break;
											}
											if (key.length() >= 4 && key.charAt(0) == '\n' && key.charAt(1) == '$') {
												final char c1 = key.charAt(2);
												final char c2 = key.charAt(3);
												if (c1 == 'S' && c2 == 'I') {
													synchronized (cache) {
														cache.setByKey(key, Leaf.readListing(din));
													}
													continue;
												}
												if (c1 == 'S' && c2 == 'i') {
													synchronized (cache) {
														cache.setByKey(key, Leaf.readListing(din));
													}
													continue;
												}
												if (c1 == 'T' && c2 == 'R') {
													if (size < 1024 && (cache.treeData == null || cache.treeData.get() == null)) {
														din.setReadLimit(size);
														cache.treeData = new SoftReference<>(Leaf.readTreeData(identifier, din));
														continue;
													}
													break;
												}
												if (c1 == 'I' && c2 == 't') {
													din.setReadLimit(size);
													result = Leaf.readLinkData(identifier, din);
													cache.linkData = new SoftReference<>(result);
													break;
												}
											}
											din.skipBytes(size);
										}
									}
								} else {
									rewrite = true;
								}
							}
						} catch (final IOException e) {
							rewrite = true;
						} finally {
							din.close();
						}
					} catch (final IOException e) {
						if (serialized.isDirectory()) {
							Leaf.cleanRecursive(serialized);
						}
					}
				}
			}
			if (!load) {
				if (cache.linkData == Leaf.linkUndefined) {
					return null;
				}
				if (cache.linkData == Leaf.linkLoading) {
					synchronized (cache) {
						while (cache.linkData == Leaf.linkLoading) {
							try {
								cache.wait(0L);
							} catch (final InterruptedException e) {
								throw new RuntimeException(e);
							}
						}
					}
				}
				return cache.linkData.get();
			}
			if (expired) {
				this.stsReadLinkExpired++;
			}
			if (result == null) {
				result = this.parent.getLink(identifier);
				if (result != null) {
					synchronized (this.dataIn) {
						folder.mkdirs();
						if (!rewrite) {
							rewrite = !serialized.exists();
						}
						this.stsWriteLink++;
						try (final RandomAccessFile dio = this.openForUpdate(serialized)) {
							this.dataOut.setOutput(dio);
							if (!rewrite) {
								dio.writeInt(Leaf.RT_INCOMPLETE);
								dio.seek(8);
								dio.writeLong(Engine.fastTime());
								dio.seek(dio.length());
							} else {
								this.dataOut.writeInt(Leaf.RT_INCOMPLETE);
								Leaf.writeExpiration(this.dataOut, Leaf.CACHE_TTL);
								this.dataOut.writeLong(Engine.fastTime());
								this.dataOut.writeLong(Engine.fastTime());
							}
							this.dataOut.writeUTF("\n$It");
							this.dataOut.flush();
							final long sizePosition = dio.getFilePointer();
							this.dataOut.writeInt(-1);
							BinaryFormat.writeLinkData(this.dataOut, result);
							this.dataOut.close();
							final long endPosition = dio.getFilePointer();
							if (rewrite) {
								dio.setLength(endPosition);
							}
							dio.seek(sizePosition);
							dio.writeInt((int) (endPosition - sizePosition - 4));
							dio.seek(0);
							dio.writeInt(Leaf.RT_MAP);
						}
					}
					Report.info(
							Leaf.OWNER,
							(rewrite
								? "Load link, imported/created: guid="
								: "Load link, imported/appended: guid=") + identifier + ", name=" + result.lnkName + ", size=" + Format.Compact.toBytes(serialized.length())
									+ ", took=" + Format.Compact.toPeriod(System.currentTimeMillis() - startDate));
				}
			}
			if (cache.linkData == Leaf.linkLoading) {
				cache.linkData = result == null
					? Leaf.linkUndefined
					: new SoftReference<>(result);
			}
			return result;
		} catch (final Error e) {
			if (load && cache != null) {
				if (cache.linkData == Leaf.linkLoading) {
					cache.linkData = Leaf.linkUndefined;
				}
				this.server.enqueueDiscard(identifier, InvalidationEventType.ULINK);
			}
			throw e;
		} catch (final RuntimeException e) {
			if (load && cache != null) {
				if (cache.linkData == Leaf.linkLoading) {
					cache.linkData = Leaf.linkUndefined;
				}
				this.server.enqueueDiscard(identifier, InvalidationEventType.ULINK);
			}
			throw e;
		} catch (final Throwable e) {
			if (load && cache != null) {
				if (cache.linkData == Leaf.linkLoading) {
					cache.linkData = Leaf.linkUndefined;
				}
				this.server.enqueueDiscard(identifier, InvalidationEventType.ULINK);
			}
			throw new RuntimeException(e);
		} finally {
			if (load && cache != null) {
				synchronized (cache) {
					cache.notifyAll();
				}
			}
		}
	}
	
	final TreeData getTreeData(final int folderIndex, final String identifier) {
		
		try {
			final CacheEntry cache = this.cache.get(identifier);
			if (cache != null) {
				if (cache.treeData != null) {
					if (cache.treeData == Leaf.treeLoading) {
						synchronized (cache) {
							while (cache.treeData == Leaf.treeLoading) {
								try {
									cache.wait(0L);
								} catch (final InterruptedException e) {
									throw new RuntimeException(e);
								}
							}
						}
					}
					final Reference<TreeData> reference = cache.treeData;
					if (reference != null) {
						if (reference == Leaf.treeUndefined) {
							return null;
						}
						final TreeData ready = reference.get();
						if (ready != null && !ready.invalid) {
							return ready;
						}
					}
				}
			}
		} catch (final ConcurrentModificationException e) {
			Report.info(Leaf.OWNER, "Extra concurrent modification: guid=" + identifier);
		}
		
		final long startDate = System.currentTimeMillis();
		boolean load = false;
		CacheEntry cache = null;
		try {
			final File folder;
			final File serialized;
			TreeData result = null;
			boolean expired = false;
			boolean rewrite = false;
			synchronized (this.dataIn) {
				cache = this.cache.get(identifier);
				if (cache != null) {
					if (cache.treeData == Leaf.treeUndefined) {
						return null;
					}
					if (cache.treeData != Leaf.treeLoading) {
						if (cache.treeData == null) {
							cache.treeData = Leaf.treeLoading;
							load = true;
						} else {
							result = cache.treeData.get();
							if (result != null && !result.invalid) {
								return result;
							}
							cache.treeData = Leaf.treeLoading;
							load = true;
						}
					}
				} else {
					cache = new CacheEntry();
					cache.treeData = Leaf.treeLoading;
					this.cache.put(identifier, cache);
					load = true;
				}
				folder = this.getCheckFolderObject(folderIndex);
				serialized = new File(folder, identifier);
				if (load) {
					this.stsReadTree++;
					try {
						final DataInputBufferedReusable din = this.dataIn;
						din.setStream(new FileInputStream(serialized));
						try {
							final int version = din.readInt(); // version
							if (version == Leaf.RT_MAP) {
								if (din.readLong() < Engine.fastTime()) { // expire
									expired = true;
								}
								din.readLong(); // updated
								if (din.readLong() < this.storage.getPresenceValidityDate()) { // created
									expired = true;
									rewrite = true;
								} else {
									while (din.available() > 0) {
										final String key = din.readUTF();
										final int size = din.readInt();
										if (size <= 0) {
											rewrite = true;
											break;
										}
										if (key.length() >= 4 && key.charAt(0) == '\n' && key.charAt(1) == '$') {
											final char c1 = key.charAt(2);
											final char c2 = key.charAt(3);
											if (c1 == 'I' && c2 == 't' && (cache.linkData == null || cache.linkData.get() == null)) {
												cache.linkData = new SoftReference<>(Leaf.readLinkData(identifier, din));
												continue;
											}
											if (c1 == 'T' && c2 == 'R') {
												din.setReadLimit(size);
												result = Leaf.readTreeData(identifier, din);
												cache.treeData = new SoftReference<>(result);
												break;
											}
										}
										din.skipBytes(size);
									}
								}
							} else {
								rewrite = true;
							}
						} catch (final IOException e) {
							rewrite = true;
						} finally {
							din.close();
						}
					} catch (final IOException e) {
						if (serialized.isDirectory()) {
							Leaf.cleanRecursive(serialized);
						}
					}
				}
			}
			if (!load) {
				if (cache.treeData == Leaf.treeUndefined) {
					return null;
				}
				if (cache.treeData == Leaf.treeLoading) {
					synchronized (cache) {
						while (cache.treeData == Leaf.treeLoading) {
							try {
								cache.wait(0L);
							} catch (final InterruptedException e) {
								throw new RuntimeException(e);
							}
						}
					}
				}
				return cache.treeData.get();
			}
			if (expired) {
				this.stsReadTreeExpired++;
			}
			if (result == null) {
				result = this.parent.getTree(identifier);
				if (result != null) {
					final LinkData[] tree = result.tree;
					synchronized (this.dataIn) {
						folder.mkdirs();
						if (!rewrite) {
							rewrite = !serialized.exists();
						}
						this.stsWriteTree++;
						try (final RandomAccessFile dio = this.openForUpdate(serialized)) {
							this.dataOut.setOutput(dio);
							if (!rewrite) {
								dio.writeInt(Leaf.RT_INCOMPLETE);
								dio.seek(8);
								dio.writeLong(Engine.fastTime());
								dio.seek(dio.length());
							} else {
								this.dataOut.writeInt(Leaf.RT_INCOMPLETE);
								Leaf.writeExpiration(this.dataOut, Leaf.CACHE_TTL);
								this.dataOut.writeLong(Engine.fastTime());
								this.dataOut.writeLong(Engine.fastTime());
							}
							this.dataOut.writeUTF("\n$TR");
							this.dataOut.flush();
							final long sizePosition = dio.getFilePointer();
							this.dataOut.writeInt(-1);
							
							BinaryFormat.writeTreeLinkArray(this.dataOut, tree);
							
							this.dataOut.close();
							final long endPosition = dio.getFilePointer();
							if (rewrite) {
								dio.setLength(endPosition);
							}
							dio.seek(sizePosition);
							dio.writeInt((int) (endPosition - sizePosition - 4));
							dio.seek(0);
							dio.writeInt(Leaf.RT_MAP);
						}
					}
					Report.info(
							Leaf.OWNER,
							(rewrite
								? "Load tree, imported/created: guid="
								: "Load tree, imported/appended: guid=") + identifier + ", count=" + tree.length + ", size=" + Format.Compact.toBytes(serialized.length())
									+ ", took=" + Format.Compact.toPeriod(System.currentTimeMillis() - startDate));
					
				}
			}
			if (cache.treeData == Leaf.treeLoading) {
				cache.treeData = result == null
					? Leaf.treeUndefined
					: new SoftReference<>(result);
			}
			return result;
		} catch (final Error e) {
			if (load && cache != null) {
				if (cache.treeData == Leaf.treeLoading) {
					cache.treeData = Leaf.treeUndefined;
				}
				this.server.enqueueDiscard(identifier, InvalidationEventType.UTREE);
			}
			throw e;
		} catch (final RuntimeException e) {
			if (load && cache != null) {
				if (cache.treeData == Leaf.treeLoading) {
					cache.treeData = Leaf.treeUndefined;
				}
				this.server.enqueueDiscard(identifier, InvalidationEventType.UTREE);
			}
			throw e;
		} catch (final Throwable e) {
			if (load && cache != null) {
				if (cache.treeData == Leaf.treeLoading) {
					cache.treeData = Leaf.treeUndefined;
				}
				this.server.enqueueDiscard(identifier, InvalidationEventType.UTREE);
			}
			throw new RuntimeException(e);
		} finally {
			if (load && cache != null) {
				synchronized (cache) {
					cache.notifyAll();
				}
			}
		}
	}
	
	final boolean hasExternal(final int folderIndex, final String identifier) {
		
		synchronized (this.dataIn) {
			final File folder = this.getCheckFolderObject(folderIndex);
			final File target = new File(folder, identifier);
			if (target.isFile()) {
				return true;
			}
		}
		return false;
	}
	
	final void invalidateDeleteExtra(final int folderIndex, final String identifier) {
		
		synchronized (this.dataIn) {
			try {
				final File folder = this.getCheckFolderObject(folderIndex);
				final File target = new File(folder, identifier);
				if (target.isFile()) {
					Report.info(
							Leaf.OWNER,
							"Extra invalidate (-extr): guid=" + identifier + ", life=" + Format.Compact.toPeriod(System.currentTimeMillis() - target.lastModified()) + ", size="
									+ Format.Compact.toBytes(target.length()));
					if (!target.delete()) {
						Report.info(
								Leaf.OWNER,
								"Extra invalidate (-extr) fail - will retry: guid=" + identifier + ", life="
										+ Format.Compact.toPeriod(System.currentTimeMillis() - target.lastModified()) + ", size=" + Format.Compact.toBytes(target.length()));
						Act.later(null, new InvalidatorRetry(target), 500L);
					}
				} else {
					Report.info(Leaf.OWNER, "Extra invalidate (-extr): guid=" + identifier + ", skipped - not cached at the moment");
				}
			} finally {
				this.invalidateCacheEntry(identifier, true);
			}
		}
	}
	
	final void invalidateDeleteLink(final int folderIndex, final String identifier) {
		
		synchronized (this.dataIn) {
			try {
				final File folder = this.getCheckFolderObject(folderIndex);
				final File target = new File(folder, identifier);
				if (target.isFile()) {
					Report.info(
							Leaf.OWNER,
							"Extra invalidate (-link): guid=" + identifier + ", life=" + Format.Compact.toPeriod(System.currentTimeMillis() - target.lastModified()) + ", size="
									+ Format.Compact.toBytes(target.length()));
					if (!target.delete()) {
						Report.info(
								Leaf.OWNER,
								"Extra invalidate (-link) fail - will retry: guid=" + identifier + ", life="
										+ Format.Compact.toPeriod(System.currentTimeMillis() - target.lastModified()) + ", size=" + Format.Compact.toBytes(target.length()));
						Act.later(null, new InvalidatorRetry(target), 500L);
					}
				} else {
					Report.info(Leaf.OWNER, "Extra invalidate (-link): guid=" + identifier + ", skipped - not cached at the moment");
				}
			} finally {
				this.invalidateCacheEntry(identifier, true);
			}
		}
	}
	
	final void invalidateUpdateIden(final int folderIndex, final String identifier) {
		
		synchronized (this.dataIn) {
			try {
				final File folder = this.getCheckFolderObject(folderIndex);
				final File target = new File(folder, identifier);
				if (target.isFile()) {
					Report.info(
							Leaf.OWNER,
							"Extra invalidate (*iden): guid=" + identifier + ", life=" + Format.Compact.toPeriod(System.currentTimeMillis() - target.lastModified()) + ", size="
									+ Format.Compact.toBytes(target.length()));
					if (!target.delete()) {
						Report.info(
								Leaf.OWNER,
								"Extra invalidate (*iden) fail - will retry: guid=" + identifier + ", life="
										+ Format.Compact.toPeriod(System.currentTimeMillis() - target.lastModified()) + ", size=" + Format.Compact.toBytes(target.length()));
						Act.later(null, new InvalidatorRetry(target), 500L);
					}
				} else {
					Report.info(Leaf.OWNER, "Extra invalidate (*iden): guid=" + identifier + ", skipped - not cached at the moment");
				}
			} finally {
				this.invalidateCacheEntry(identifier, true);
			}
		}
	}
	
	final void invalidateUpdateLink(final int folderIndex, final String identifier) {
		
		synchronized (this.dataIn) {
			try {
				final File folder = this.getCheckFolderObject(folderIndex);
				final File target = new File(folder, identifier);
				if (target.isFile()) {
					final long age = System.currentTimeMillis() - target.lastModified();
					Report.info(//
							Leaf.OWNER,
							"Extra invalidate (*link): guid=" + identifier + //
									", life=" + Format.Compact.toPeriod(age) + //
									", size=" + Format.Compact.toBytes(target.length())//
					);
					if (!target.delete()) {
						Report.info(//
								Leaf.OWNER,
								"Extra invalidate (*link) fail - will retry: guid=" + identifier + //
										", life=" + Format.Compact.toPeriod(age) + //
										", size=" + Format.Compact.toBytes(target.length())//
						);
						Act.later(null, new InvalidatorRetry(target), 500L);
					}
				} else {
					Report.info(Leaf.OWNER, "Extra invalidate (*link): guid=" + identifier + ", skipped - not cached at the moment");
				}
			} finally {
				this.invalidateCacheEntry(identifier, false);
			}
		}
	}
	
	final void invalidateUpdateTree(final int folderIndex, final String identifier) {
		
		synchronized (this.dataIn) {
			try {
				final File folder = this.getCheckFolderObject(folderIndex);
				final File target = new File(folder, identifier);
				if (target.isFile()) {
					Report.info(
							Leaf.OWNER,
							"Extra invalidate (*tree): guid=" + identifier + ", life=" + Format.Compact.toPeriod(System.currentTimeMillis() - target.lastModified()) + ", size="
									+ Format.Compact.toBytes(target.length()));
					if (!target.delete()) {
						Report.info(
								Leaf.OWNER,
								"Extra invalidate (*tree) fail - will retry: guid=" + identifier + ", life="
										+ Format.Compact.toPeriod(System.currentTimeMillis() - target.lastModified()) + ", size=" + Format.Compact.toBytes(target.length()));
						Act.later(null, new InvalidatorRetry(target), 500L);
					}
				} else {
					Report.info(Leaf.OWNER, "Extra invalidate (*tree): guid=" + identifier + ", skipped - not cached at the moment");
				}
			} finally {
				this.invalidateCacheEntry(identifier, false);
			}
		}
	}
	
	final void putCopier(final int folderIndex, final String identifier, final String type, final TransferCopier copier) throws Exception {
		
		synchronized (this.dataIn) {
			final File folder = this.getCheckFolderObject(folderIndex);
			folder.mkdirs();
			final File target = new File(folder, identifier);
			if (target.isDirectory()) {
				Leaf.cleanRecursive(target);
			}
			this.stsWriteExternal++;
			try (final RandomAccessFile out = this.openForUpdate(target)) {
				this.dataOut.setOutput(out);
				this.dataOut.writeInt(Leaf.RT_INCOMPLETE);
				Leaf.writeExpiration(this.dataOut, Leaf.CACHE_EXTRA);
				this.dataOut.writeLong(Engine.fastTime());
				this.dataOut.writeLong(Engine.fastTime());
				this.dataOut.writeUTF(type);
				this.dataOut.writeInt(this.dataOut.getWrittenByteCount() + 4);
				Transfer.toStream(copier.nextCopy(), this.dataOut, false);
				this.dataOut.close();
				out.setLength(out.getFilePointer());
				out.seek(0L);
				out.writeInt(Leaf.RT_EXTRA);
			}
		}
	}
	
	final LinkData[] searchIdentity(final int folderIndex, final String identifier) {
		
		final String searchKey = "\n$Si";
		try {
			final CacheEntry cache = this.cache.get(identifier);
			if (cache != null) {
				LinkData[] ready;
				ready = cache.getByKey(searchKey);
				if (ready == Leaf.listLoading) {
					synchronized (cache) {
						ready = cache.getByKey(searchKey);
						while (ready == Leaf.listLoading) {
							try {
								cache.wait(0L);
							} catch (final InterruptedException e) {
								throw new RuntimeException(e);
							}
							ready = cache.getByKey(searchKey);
						}
					}
				}
				if (ready == Leaf.listUndefined) {
					return null;
				}
				if (ready != null) {
					return ready;
				}
			}
		} catch (final ConcurrentModificationException e) {
			Report.info(Leaf.OWNER, "Extra concurrent modification: guid=" + identifier);
		}
		
		final long startDate = System.currentTimeMillis();
		boolean load = false;
		CacheEntry cache = null;
		try {
			final File folder;
			final File serialized;
			LinkData[] result = null;
			boolean expired = false;
			boolean rewrite = false;
			synchronized (this.dataIn) {
				cache = this.cache.get(identifier);
				if (cache != null) {
					LinkData[] ready;
					ready = cache.getByKey(searchKey);
					if (ready == Leaf.listUndefined) {
						return null;
					}
					if (ready != Leaf.listLoading) {
						if (ready != null) {
							return ready;
						}
						cache.setByKey(searchKey, Leaf.listLoading);
						load = true;
					}
				} else {
					cache = new CacheEntry();
					cache.setByKey(searchKey, Leaf.listLoading);
					this.cache.put(identifier, cache);
					load = true;
				}
				folder = this.getCheckFolderObject(folderIndex);
				serialized = new File(folder, identifier);
				if (load) {
					this.stsReadSearchIdentity++;
					try {
						final DataInputBufferedReusable din = this.dataIn;
						din.setStream(new FileInputStream(serialized));
						try {
							final int version = din.readInt(); // version
							if (version == Leaf.RT_MAP) {
								if (din.readLong() > Engine.fastTime()) { // expire
									expired = true;
								}
								din.readLong(); // updated
								if (din.readLong() < this.storage.getPresenceValidityDate()) { // created
									expired = true;
									rewrite = true;
								} else {
									while (din.available() > 0) {
										final String key = din.readUTF();
										final int size = din.readInt();
										if (size <= 0) {
											rewrite = true;
											break;
										}
										if (searchKey.equals(key)) {
											din.setReadLimit(size);
											result = Leaf.readListing(din);
											break;
										}
										din.skipBytes(size);
									}
								}
							} else {
								rewrite = true;
							}
						} catch (final IOException e) {
							rewrite = true;
						} finally {
							din.close();
						}
					} catch (final IOException e) {
						if (serialized.isDirectory()) {
							Leaf.cleanRecursive(serialized);
						}
					}
				}
			}
			if (!load) {
				LinkData[] ready;
				ready = cache.getByKey(searchKey);
				if (ready == Leaf.listLoading) {
					synchronized (cache) {
						ready = cache.getByKey(searchKey);
						while (ready == Leaf.listLoading) {
							try {
								cache.wait(0L);
							} catch (final InterruptedException e) {
								throw new RuntimeException(e);
							}
							ready = cache.getByKey(searchKey);
						}
					}
				}
				if (ready == Leaf.listUndefined) {
					return null;
				}
			}
			if (expired) {
				this.stsReadSearchIdentityExpired++;
			}
			if (result == null) {
				result = this.parent.searchIdentity(identifier);
				if (result != null) {
					synchronized (this.dataIn) {
						folder.mkdirs();
						if (!rewrite) {
							rewrite = !serialized.exists();
						}
						this.stsWriteSearchIdentity++;
						try (final RandomAccessFile dio = this.openForUpdate(serialized)) {
							this.dataOut.setOutput(dio);
							if (!rewrite) {
								dio.writeInt(Leaf.RT_INCOMPLETE);
								dio.seek(8);
								dio.writeLong(Engine.fastTime());
								dio.seek(dio.length());
							} else {
								this.dataOut.writeInt(Leaf.RT_INCOMPLETE);
								Leaf.writeExpiration(this.dataOut, Leaf.CACHE_TTL);
								this.dataOut.writeLong(Engine.fastTime());
								this.dataOut.writeLong(Engine.fastTime());
							}
							this.dataOut.writeUTF(searchKey);
							this.dataOut.flush();
							final long sizePosition = dio.getFilePointer();
							this.dataOut.writeInt(-1);
							BinaryFormat.writeListing(result, this.dataOut);
							this.dataOut.close();
							final long endPosition = dio.getFilePointer();
							if (rewrite) {
								dio.setLength(endPosition);
							}
							dio.seek(sizePosition);
							dio.writeInt((int) (endPosition - sizePosition - 4));
							dio.seek(0);
							dio.writeInt(Leaf.RT_MAP);
						}
					}
					Report.info(
							Leaf.OWNER,
							(rewrite
								? "Search identity, imported/created: guid="
								: "Search identity, imported/appended: guid=") + identifier + ", searchKey=" + searchKey.substring(1) + ", count=" + result.length + ", size="
									+ Format.Compact.toBytes(serialized.length()) + ", took=" + Format.Compact.toPeriod(System.currentTimeMillis() - startDate));
					
				}
			}
			cache.setByKey(
					searchKey,
					result == null
						? Leaf.listUndefined
						: result);
			return result;
		} catch (final Error e) {
			if (load && cache != null) {
				cache.setByKey(searchKey, Leaf.listUndefined);
				this.server.enqueueDiscard(identifier, InvalidationEventType.UTREE);
			}
			throw e;
		} catch (final RuntimeException e) {
			if (load && cache != null) {
				cache.setByKey(searchKey, Leaf.listUndefined);
				this.server.enqueueDiscard(identifier, InvalidationEventType.UTREE);
			}
			throw e;
		} catch (final Throwable e) {
			if (load && cache != null) {
				cache.setByKey(searchKey, Leaf.listUndefined);
				this.server.enqueueDiscard(identifier, InvalidationEventType.UTREE);
			}
			throw new RuntimeException(e);
		} finally {
			if (load && cache != null) {
				synchronized (cache) {
					cache.notifyAll();
				}
			}
		}
	}
	
	final LinkData[] searchLinks(final int folderIndex, final String identifier) {
		
		final String searchKey = "\n$So";
		
		try {
			final CacheEntry cache = this.cache.get(identifier);
			if (cache != null) {
				LinkData[] ready;
				ready = cache.getByKey(searchKey);
				if (ready == Leaf.listLoading) {
					synchronized (cache) {
						ready = cache.getByKey(searchKey);
						while (ready == Leaf.listLoading) {
							try {
								cache.wait(0L);
							} catch (final InterruptedException e) {
								throw new RuntimeException(e);
							}
							ready = cache.getByKey(searchKey);
						}
					}
				}
				if (ready == Leaf.listUndefined) {
					return null;
				}
				if (ready != null) {
					return ready;
				}
			}
		} catch (final ConcurrentModificationException e) {
			Report.info(Leaf.OWNER, "Extra concurrent modification: guid=" + identifier);
		}
		
		final long startDate = System.currentTimeMillis();
		boolean load = false;
		CacheEntry cache = null;
		try {
			final File folder;
			final File serialized;
			LinkData[] result = null;
			boolean expired = false;
			boolean rewrite = false;
			synchronized (this.dataIn) {
				cache = this.cache.get(identifier);
				if (cache != null) {
					LinkData[] ready;
					ready = cache.getByKey(searchKey);
					if (ready == Leaf.listUndefined) {
						return null;
					}
					if (ready != Leaf.listLoading) {
						if (ready != null) {
							return ready;
						}
						cache.setByKey(searchKey, Leaf.listLoading);
						load = true;
					}
				} else {
					cache = new CacheEntry();
					cache.setByKey(searchKey, Leaf.listLoading);
					this.cache.put(identifier, cache);
					load = true;
				}
				folder = this.getCheckFolderObject(folderIndex);
				serialized = new File(folder, identifier);
				if (load) {
					this.stsReadSearchLinks++;
					try {
						final DataInputBufferedReusable din = this.dataIn;
						din.setStream(new FileInputStream(serialized));
						try {
							final int version = din.readInt(); // version
							if (version == Leaf.RT_MAP) {
								if (din.readLong() > Engine.fastTime()) { // expire
									expired = true;
								}
								din.readLong(); // updated
								if (din.readLong() < this.storage.getPresenceValidityDate()) { // created
									expired = true;
									rewrite = true;
								} else {
									while (din.available() > 0) {
										final String key = din.readUTF();
										final int size = din.readInt();
										if (size <= 0) {
											rewrite = true;
											break;
										}
										if (searchKey.equals(key)) {
											din.setReadLimit(size);
											result = Leaf.readListing(din);
											break;
										}
										din.skipBytes(size);
									}
								}
							} else {
								rewrite = true;
							}
						} catch (final IOException e) {
							rewrite = true;
						} finally {
							din.close();
						}
					} catch (final IOException e) {
						if (serialized.isDirectory()) {
							Leaf.cleanRecursive(serialized);
						}
					}
				}
			}
			if (!load) {
				LinkData[] ready;
				ready = cache.getByKey(searchKey);
				if (ready == Leaf.listLoading) {
					synchronized (cache) {
						ready = cache.getByKey(searchKey);
						while (ready == Leaf.listLoading) {
							try {
								cache.wait(0L);
							} catch (final InterruptedException e) {
								throw new RuntimeException(e);
							}
							ready = cache.getByKey(searchKey);
						}
					}
				}
				if (ready == Leaf.listUndefined) {
					return null;
				}
			}
			if (expired) {
				this.stsReadSearchLinksExpired++;
			}
			if (result == null) {
				result = this.parent.searchLinks(identifier);
				if (result != null) {
					synchronized (this.dataIn) {
						folder.mkdirs();
						if (!rewrite) {
							rewrite = !serialized.exists();
						}
						this.stsWriteSearchLinks++;
						try (final RandomAccessFile dio = this.openForUpdate(serialized)) {
							this.dataOut.setOutput(dio);
							if (!rewrite) {
								dio.writeInt(Leaf.RT_INCOMPLETE);
								dio.seek(8);
								dio.writeLong(Engine.fastTime());
								dio.seek(dio.length());
							} else {
								this.dataOut.writeInt(Leaf.RT_INCOMPLETE);
								Leaf.writeExpiration(this.dataOut, Leaf.CACHE_TTL);
								this.dataOut.writeLong(Engine.fastTime());
								this.dataOut.writeLong(Engine.fastTime());
							}
							this.dataOut.writeUTF(searchKey);
							this.dataOut.flush();
							final long sizePosition = dio.getFilePointer();
							this.dataOut.writeInt(-1);
							BinaryFormat.writeListing(result, this.dataOut);
							this.dataOut.close();
							final long endPosition = dio.getFilePointer();
							if (rewrite) {
								dio.setLength(endPosition);
							}
							dio.seek(sizePosition);
							dio.writeInt((int) (endPosition - sizePosition - 4));
							dio.seek(0);
							dio.writeInt(Leaf.RT_MAP);
						}
					}
					Report.info(
							Leaf.OWNER,
							(rewrite
								? "Search links, imported/created: guid="
								: "Search links, imported/appended: guid=") + identifier + ", searchKey=" + searchKey.substring(1) + ", count=" + result.length + ", size="
									+ Format.Compact.toBytes(serialized.length()) + ", took=" + Format.Compact.toPeriod(System.currentTimeMillis() - startDate));
					
				}
			}
			cache.setByKey(
					searchKey,
					result == null
						? Leaf.listUndefined
						: result);
			return result;
		} catch (final Error e) {
			if (load && cache != null) {
				cache.setByKey(searchKey, Leaf.listUndefined);
				this.server.enqueueDiscard(identifier, InvalidationEventType.UTREE);
			}
			throw e;
		} catch (final RuntimeException e) {
			if (load && cache != null) {
				cache.setByKey(searchKey, Leaf.listUndefined);
				this.server.enqueueDiscard(identifier, InvalidationEventType.UTREE);
			}
			throw e;
		} catch (final Throwable e) {
			if (load && cache != null) {
				cache.setByKey(searchKey, Leaf.listUndefined);
				this.server.enqueueDiscard(identifier, InvalidationEventType.UTREE);
			}
			throw new RuntimeException(e);
		} finally {
			if (load && cache != null) {
				synchronized (cache) {
					cache.notifyAll();
				}
			}
		}
	}
	
	final LinkData[] searchLocal(final int folderIndex, final String identifier, final String condition) {
		
		final String searchKey = "\n$sL/" + (condition == null
			? ""
			: condition);
		try {
			final CacheEntry cache = this.cache.get(identifier);
			if (cache != null) {
				LinkData[] ready;
				ready = cache.getByKey(searchKey);
				if (ready == Leaf.listLoading) {
					synchronized (cache) {
						ready = cache.getByKey(searchKey);
						while (ready == Leaf.listLoading) {
							try {
								cache.wait(0L);
							} catch (final InterruptedException e) {
								throw new RuntimeException(e);
							}
							ready = cache.getByKey(searchKey);
						}
					}
				}
				if (ready == Leaf.listUndefined) {
					return null;
				}
				if (ready != null) {
					return ready;
				}
			}
		} catch (final ConcurrentModificationException e) {
			Report.info(Leaf.OWNER, "Extra concurrent modification: guid=" + identifier);
		}
		
		final long startDate = System.currentTimeMillis();
		boolean load = false;
		CacheEntry cache = null;
		try {
			final File folder;
			final File serialized;
			LinkData[] result = null;
			boolean expired = false;
			boolean rewrite = false;
			synchronized (this.dataIn) {
				cache = this.cache.get(identifier);
				if (cache != null) {
					LinkData[] ready;
					ready = cache.getByKey(searchKey);
					if (ready == Leaf.listUndefined) {
						return null;
					}
					if (ready != Leaf.listLoading) {
						if (ready != null) {
							return ready;
						}
						cache.setByKey(searchKey, Leaf.listLoading);
						load = true;
					}
				} else {
					cache = new CacheEntry();
					cache.setByKey(searchKey, Leaf.listLoading);
					this.cache.put(identifier, cache);
					load = true;
				}
				folder = this.getCheckFolderObject(folderIndex);
				serialized = new File(folder, identifier);
				if (load) {
					this.stsReadSearchLocal++;
					try {
						final DataInputBufferedReusable din = this.dataIn;
						din.setStream(new FileInputStream(serialized));
						try {
							final int version = din.readInt(); // version
							if (version == Leaf.RT_MAP) {
								if (din.readLong() < Engine.fastTime()) { // expire
									expired = true;
								}
								din.readLong(); // updated
								if (din.readLong() < this.storage.getPresenceValidityDate()) { // created
									expired = true;
									rewrite = true;
								} else {
									while (din.available() > 0) {
										final String key = din.readUTF();
										final int size = din.readInt();
										if (size <= 0) {
											rewrite = true;
											break;
										}
										if (searchKey.equals(key)) {
											din.setReadLimit(size);
											result = Leaf.readListing(din);
											break;
										}
										din.skipBytes(size);
									}
								}
							} else {
								rewrite = true;
							}
						} catch (final IOException e) {
							rewrite = true;
						} finally {
							din.close();
						}
					} catch (final IOException e) {
						if (serialized.isDirectory()) {
							Leaf.cleanRecursive(serialized);
						}
					}
				}
			}
			if (!load) {
				LinkData[] ready;
				ready = cache.getByKey(searchKey);
				if (ready == Leaf.listLoading) {
					synchronized (cache) {
						ready = cache.getByKey(searchKey);
						while (ready == Leaf.listLoading) {
							try {
								cache.wait(0L);
							} catch (final InterruptedException e) {
								throw new RuntimeException(e);
							}
							ready = cache.getByKey(searchKey);
						}
					}
				}
				if (ready == Leaf.listUndefined) {
					return null;
				}
			}
			if (expired) {
				this.stsReadSearchLocalExpired++;
			}
			if (result == null) {
				result = this.parent.searchLocal(
						identifier,
						condition == null
							? ""
							: condition);
				if (result != null) {
					synchronized (this.dataIn) {
						folder.mkdirs();
						if (!rewrite) {
							rewrite = !serialized.exists();
						}
						this.stsWriteSearchLocal++;
						try (final RandomAccessFile dio = this.openForUpdate(serialized)) {
							this.dataOut.setOutput(dio);
							if (!rewrite) {
								dio.writeInt(Leaf.RT_INCOMPLETE);
								dio.seek(8);
								dio.writeLong(Engine.fastTime());
								dio.seek(dio.length());
							} else {
								this.dataOut.writeInt(Leaf.RT_INCOMPLETE);
								Leaf.writeExpiration(this.dataOut, Leaf.CACHE_TTL);
								this.dataOut.writeLong(Engine.fastTime());
								this.dataOut.writeLong(Engine.fastTime());
							}
							this.dataOut.writeUTF(searchKey);
							this.dataOut.flush();
							final long sizePosition = dio.getFilePointer();
							this.dataOut.writeInt(-1);
							BinaryFormat.writeListing(result, this.dataOut);
							this.dataOut.close();
							final long endPosition = dio.getFilePointer();
							if (rewrite) {
								dio.setLength(endPosition);
							}
							dio.seek(sizePosition);
							dio.writeInt((int) (endPosition - sizePosition - 4));
							dio.seek(0);
							dio.writeInt(Leaf.RT_MAP);
						}
					}
					Report.info(
							Leaf.OWNER,
							(rewrite
								? "Search local, imported/created: guid="
								: "Search local, imported/appended: guid=") + identifier + ", searchKey=" + searchKey.substring(1) + ", count=" + result.length + ", size="
									+ Format.Compact.toBytes(serialized.length()) + ", took=" + Format.Compact.toPeriod(System.currentTimeMillis() - startDate));
				}
			}
			cache.setByKey(
					searchKey,
					result == null
						? Leaf.listUndefined
						: result);
			return result;
		} catch (final Error e) {
			if (load && cache != null) {
				cache.setByKey(searchKey, Leaf.listUndefined);
				this.server.enqueueDiscard(identifier, InvalidationEventType.UTREE);
			}
			throw e;
		} catch (final RuntimeException e) {
			if (load && cache != null) {
				cache.setByKey(searchKey, Leaf.listUndefined);
				this.server.enqueueDiscard(identifier, InvalidationEventType.UTREE);
			}
			throw e;
		} catch (final Throwable e) {
			if (load && cache != null) {
				cache.setByKey(searchKey, Leaf.listUndefined);
				this.server.enqueueDiscard(identifier, InvalidationEventType.UTREE);
			}
			throw new RuntimeException(e);
		} finally {
			if (load && cache != null) {
				synchronized (cache) {
					cache.notifyAll();
				}
			}
		}
	}
	
	final void statusFill(final StatusContainer container) {
		
		container.stsCacheSize += this.cache.size();
		container.stsCheckFile += this.stsCheckFile;
		container.stsCheckExpired += this.stsCheckExpired;
		container.stsCheckRequests += this.stsCheckRequests;
		container.stsReadLink += this.stsReadLink;
		container.stsReadLinkExpired += this.stsReadLinkExpired;
		container.stsReadTree += this.stsReadTree;
		container.stsReadTreeExpired += this.stsReadTreeExpired;
		container.stsReadExternal += this.stsReadExternal;
		container.stsReadSearchIdentity += this.stsReadSearchIdentity;
		container.stsReadSearchIdentityExpired += this.stsReadSearchIdentityExpired;
		container.stsReadSearchLinks += this.stsReadSearchLinks;
		container.stsReadSearchLinksExpired += this.stsReadSearchLinksExpired;
		container.stsReadSearchLocal += this.stsReadSearchLocal;
		container.stsReadSearchLocalExpired += this.stsReadSearchLocalExpired;
		container.stsWriteLink += this.stsWriteLink;
		container.stsWriteTree += this.stsWriteTree;
		container.stsWriteExternal += this.stsWriteExternal;
		container.stsWriteSearchIdentity += this.stsWriteSearchIdentity;
		container.stsWriteSearchLinks += this.stsWriteSearchLinks;
		container.stsWriteSearchLocal += this.stsWriteSearchLocal;
	}
}
