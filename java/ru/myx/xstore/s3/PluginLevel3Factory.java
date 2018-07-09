package ru.myx.xstore.s3;

import ru.myx.ae1.AcmPluginFactory;
import ru.myx.ae1.PluginInstance;
import ru.myx.ae3.base.BaseObject;

/*
 * Created on 07.10.2003
 *
 * To change the template for this generated file go to
 * Window>Preferences>Java>Code Generation>Code and Comments
 */
/** @author myx */
public final class PluginLevel3Factory implements AcmPluginFactory {

	private static final String[] VARIETY = {
			"ACMMOD:STORAGE3"
	};

	@Override
	public final PluginInstance produce(final String variant, final BaseObject attributes, final Object source) {

		return new StorageLevel3();
	}

	@Override
	public final String[] variety() {

		return PluginLevel3Factory.VARIETY;
	}
}
