/*
 * Created on 05.07.2004
 */
package ru.myx.xstore.s3.concept;

import java.util.Iterator;

import ru.myx.ae3.base.Base;
import ru.myx.ae3.base.BaseHostSealed;
import ru.myx.ae3.base.BaseObject;
import ru.myx.ae3.base.BasePrimitive;
import ru.myx.ae3.base.BasePrimitiveString;
import ru.myx.ae3.base.BaseProperty;
import ru.myx.ae3.exec.ExecProcess;
import ru.myx.ae3.exec.ExecStateCode;
import ru.myx.ae3.exec.ResultHandler;
import ru.myx.xstore.s3.StorageLevel3;

/** @author myx
 *
 *         To change the template for this generated type comment go to
 *         Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments */
class FieldsReadable extends BaseHostSealed implements BaseProperty {

	private final StorageLevel3 storage;
	
	private final LinkData link;
	
	FieldsReadable(final StorageLevel3 storage, final LinkData link) {
		
		this.storage = storage;
		this.link = link;
	}
	
	@Override
	public BaseProperty baseGetOwnProperty(final BasePrimitiveString key) {

		if (key.length() > 0 && '$' == key.charAt(0)) {
			return this;
		}
		return this.getParent().baseFindProperty(key, BaseObject.PROTOTYPE);
	}
	
	@Override
	public BaseProperty baseGetOwnProperty(final String key) {

		if (key.length() > 0 && '$' == key.charAt(0)) {
			return this;
		}
		return this.getParent().baseFindProperty(key, BaseObject.PROTOTYPE);
	}
	
	@Override
	public boolean baseHasKeysOwn() {

		return Base.hasKeys(this.getParent());
	}
	
	@Override
	public Iterator<String> baseKeysOwn() {

		return Base.keys(this.getParent());
	}
	
	@Override
	public Iterator<? extends BasePrimitive<?>> baseKeysOwnPrimitive() {

		return Base.keysPrimitive(this.getParent());
	}
	
	@Override
	public BaseObject basePrototype() {

		/** FIXME: Should be dynamic prototype. <code>
		 super( type.getTypePrototypeObject() );
		 </code> */
		return null;
	}
	
	private BaseObject getParent() {

		return this.link.getDataReal(this.storage, null);
	}
	
	@Override
	public short propertyAttributes(final CharSequence name) {

		return BaseProperty.ATTRS_MASK_NNN_NNK;
	}
	
	@Override
	public BaseObject propertyGet(final BaseObject instance, final BasePrimitiveString key) {

		return this.propertyGet(null, key.toString());
	}
	
	@Override
	public BaseObject propertyGet(final BaseObject instance, final String key) {

		assert key != null;
		switch (key.length()) {
			case 4 :
				if (key.charAt(0) == '$') {
					if ("$key".equals(key)) {
						return Base.forString(this.link.lnkName);
					}
				}
				break;
			case 5 :
				if (key.charAt(0) == '$') {
					if ("$type".equals(key)) {
						return Base.forString(this.link.objType);
					}
					if ("$guid".equals(key)) {
						return Base.forString(this.link.lnkId);
					}
				}
				break;
			case 6 :
				if (key.charAt(0) == '$') {
					if ("$title".equals(key)) {
						return Base.forString(this.link.objTitle);
					}
					if ("$state".equals(key)) {
						return Base.forInteger(this.link.objState);
					}
					if ("$owner".equals(key)) {
						return Base.forString(this.link.objOwner);
					}
				}
				break;
			case 7 :
				if (key.charAt(0) == '$') {
					if ("$folder".equals(key)) {
						return this.link.lnkFolder
							? BaseObject.TRUE
							: BaseObject.FALSE;
					}
				}
				break;
			case 8 :
				if (key.charAt(0) == '$') {
					if ("$created".equals(key)) {
						return Base.forDateMillis(this.link.objCreated);
					}
				}
				break;
			case 9 :
				if (key.charAt(0) == '$') {
					if ("$modified".equals(key)) {
						return Base.forDateMillis(this.link.objModified);
					}
				}
				break;
			default :
		}
		return BaseObject.UNDEFINED;
	}
	
	@Override
	public BaseObject propertyGetAndSet(final BaseObject instance, final String name, final BaseObject value) {

		return this.propertyGet(null, name);
	}
	
	@Override
	public ExecStateCode propertyGetCtxResult(final ExecProcess ctx, final BaseObject instance, final BasePrimitive<?> name, final ResultHandler store) {

		return store.execReturn(ctx, this.propertyGet(null, name.toString()));
	}
}
