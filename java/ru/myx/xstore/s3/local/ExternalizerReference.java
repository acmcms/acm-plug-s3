/*
 * Created on 07.09.2004
 * 
 * Window - Preferences - Java - Code Style - Code Templates
 */
package ru.myx.xstore.s3.local;

import ru.myx.ae3.binary.TransferCopier;
import ru.myx.ae3.extra.External;
import ru.myx.ae3.extra.ExternalHandler;

/**
 * @author myx
 * 
 *         Window - Preferences - Java - Code Style - Code Templates
 */
final class ExternalizerReference implements ExternalHandler {
	private final ServerLocal	parent;
	
	ExternalizerReference(final ServerLocal parent) {
		this.parent = parent;
	}
	
	@Override
	public boolean checkIssuer(final Object issuer) {
		return this.parent.checkIssuer( issuer );
	}
	
	@Override
	public final External getExternal(final Object attachment, final String identifier) throws Exception {
		return this.parent.getExternalReference( identifier );
	}
	
	@Override
	public final boolean hasExternal(final Object attachment, final String identifier) throws Exception {
		return this.parent.hasExternal( attachment, identifier );
	}
	
	@Override
	public final String putExternal(
			final Object attachment,
			final String key,
			final String type,
			final TransferCopier copier) throws Exception {
		return this.parent.putExternal( attachment, key, type, copier );
	}
}
