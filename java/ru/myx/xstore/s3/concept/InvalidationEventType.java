/**
 *
 */
package ru.myx.xstore.s3.concept;

/** @author myx */
public enum InvalidationEventType {
	/**
	 *
	 */
	ULINK("*link") {
		
		@Override
		public final void invalidateOn(final StorageInterface iface, final String identifier) {
			
			iface.invalidateUpdateLink(identifier);
		}
	},
	/**
	 *
	 */
	DLINK("-link") {
		
		@Override
		public final void invalidateOn(final StorageInterface iface, final String identifier) {
			
			iface.invalidateDeleteLink(identifier);
		}
	},
	/**
	 *
	 */
	UTREE("*tree") {
		
		@Override
		public final void invalidateOn(final StorageInterface iface, final String identifier) {
			
			iface.invalidateUpdateTree(identifier);
		}
	},
	/**
	 *
	 */
	UIDEN("*iden") {
		
		@Override
		public final void invalidateOn(final StorageInterface iface, final String identifier) {
			
			iface.invalidateUpdateIdentity(identifier);
		}
	},
	/**
	 *
	 */
	DEXTR("-extr") {
		
		@Override
		public final void invalidateOn(final StorageInterface iface, final String identifier) {
			
			iface.invalidateDeleteExtra(identifier);
		}
	},
	/**
	 *
	 */
	UNKNOWN("") {
		
		@Override
		public final void invalidateOn(final StorageInterface iface, final String identifier) {
			
			DLINK.invalidateOn(iface, identifier);
			DEXTR.invalidateOn(iface, identifier);
		}
	},;

	/** Never return null, will return fail-over event type at least
	 *
	 * @param string
	 * @return */
	public static final InvalidationEventType getEventType(final String string) {
		
		if (string == null || string.length() < 2) {
			return UNKNOWN;
		}
		switch (string.charAt(0)) {
			case '*' : {
				switch (string.charAt(1)) {
					case 'l' : {
						if (ULINK.key.equals(string)) {
							return ULINK;
						}
						return UNKNOWN;
					}
					case 't' : {
						if (UTREE.key.equals(string)) {
							return UTREE;
						}
						return UNKNOWN;
					}
					case 'i' : {
						if (UIDEN.key.equals(string)) {
							return UIDEN;
						}
						return UNKNOWN;
					}
					default :
				}
				return UNKNOWN;
			}
			case '-' : {
				switch (string.charAt(1)) {
					case 'l' : {
						if (DLINK.key.equals(string)) {
							return DLINK;
						}
						return UNKNOWN;
					}
					case 'e' : {
						if (DEXTR.key.equals(string)) {
							return DEXTR;
						}
						return UNKNOWN;
					}
					default :
				}
				return UNKNOWN;
			}
			default :
		}
		return UNKNOWN;
	}

	private final String key;

	InvalidationEventType(final String key) {
		
		this.key = key;
	}

	/** @param iface
	 * @param identifier
	 */
	public abstract void invalidateOn(final StorageInterface iface, final String identifier);

	@Override
	public String toString() {
		
		return this.key;
	}
}
