/*
 * Created on 30.06.2004
 */
package ru.myx.xstore.s3;

import ru.myx.ae1.types.Type;
import ru.myx.ae3.base.BaseObject;
import ru.myx.xstore.s3.concept.LinkData;

/** @author myx
 *
 *         To change the template for this generated type comment go to
 *         Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments */
class EntryImplTypeOverride extends EntryImpl {

	private final String typeNameOverride;

	EntryImplTypeOverride(final StorageLevel3 storage, final LinkData link, final String typeNameOverride) {

		super(storage, link);
		this.typeNameOverride = typeNameOverride;
	}

	@Override
	public final BaseObject getData() {

		return this.getLink().getData(this.storage, this.typeNameOverride);
	}

	@Override
	public final Type<?> getType() {

		return this.storage.getServer().getTypes().getType(this.typeNameOverride);
	}

	@Override
	public final String getTypeName() {

		return this.typeNameOverride == null
			? this.getLink().objType
			: this.typeNameOverride;
	}
}
