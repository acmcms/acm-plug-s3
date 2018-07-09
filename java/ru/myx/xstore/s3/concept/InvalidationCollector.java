/**
 * 
 */
package ru.myx.xstore.s3.concept;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import ru.myx.ae3.help.Create;

/**
 * @author myx
 * 
 */
public final class InvalidationCollector {
	private Map<InvalidationEventType, Collection<String>>	invalidations;
	
	/**
	 * @param type
	 * @param guid
	 */
	public final void add(final InvalidationEventType type, final String guid) {
		if (this.invalidations == null) {
			this.invalidations = Create.treeMap();
		}
		final Collection<String> candidate = this.invalidations.get( type );
		if (candidate == null) {
			final Set<String> collection = Create.tempSet();
			collection.add( guid );
			this.invalidations.put( type, collection );
		} else {
			candidate.add( guid );
		}
	}
	
	/**
     * 
     */
	public final void clear() {
		this.invalidations = null;
	}
	
	/**
	 * @return invalidations
	 */
	public final Map<InvalidationEventType, Collection<String>> getInvalidations() {
		return this.invalidations;
	}
}
