package ru.myx.xstore.s3.local;

import java.io.DataOutput;
import java.io.IOException;

import ru.myx.xstore.s3.concept.LinkData;

final class BinaryFormat {

	static final void writeLinkData(final DataOutput dout, final LinkData link) throws Exception {

		dout.writeInt(link.lnkLuid);
		dout.writeUTF(link.lnkCntId);
		dout.writeUTF(link.lnkName);
		dout.writeBoolean(link.lnkFolder);
		dout.writeUTF(link.objId);
		dout.writeUTF(link.vrId);
		dout.writeUTF(link.objTitle);
		dout.writeLong(link.objCreated);
		dout.writeLong(link.objModified);
		dout.writeUTF(link.objOwner);
		dout.writeUTF(link.objType);
		dout.writeInt(link.objState);
		dout.writeUTF(link.extLink);
	}
	
	static final void writeListing(final LinkData[] array, final DataOutput dos) throws IOException, Exception {

		dos.writeInt(array.length);
		for (final LinkData data : array) {
			BinaryFormat.writeListingLinkData(dos, data);
		}
	}
	
	static final void writeListingLinkData(final DataOutput dout, final LinkData link) throws Exception {

		dout.writeUTF(link.lnkId);
		dout.writeBoolean(link.lnkFolder);
		dout.writeLong(link.objCreated);
		dout.writeLong(link.objModified);
		dout.writeInt(link.objState);
	}
	
	static void writeTreeLinkArray(final DataOutput dout, final LinkData[] tree) throws IOException, Exception {

		dout.writeInt(tree.length);
		for (final LinkData data : tree) {
			BinaryFormat.writeTreeLinkData(dout, data);
		}
	}
	
	static final void writeTreeLinkData(final DataOutput dout, final LinkData link) throws Exception {

		dout.writeUTF(link.lnkId);
		dout.writeUTF(link.lnkName);
		dout.writeBoolean(link.lnkFolder);
		dout.writeChar(
				link.letter == null || link.letter.isEmpty()
					? 0
					: link.letter.charAt(0));
		dout.writeLong(link.objCreated);
		dout.writeLong(link.objModified);
		dout.writeUTF(link.objType);
		dout.writeInt(link.objState);
	}
	
	/** prevent */
	private BinaryFormat() {

		//
	}
}
