/**
 * 
 */
package ru.myx.xstore.s3.local;

import ru.myx.xstore.s3.concept.InvalidationEventType;

/**
 * @author myx
 * 
 */
final class DiscardRecord {
	final long					date;
	
	final String				guid;
	
	final InvalidationEventType	invalidator;
	
	DiscardRecord(final long date, final String guid, final InvalidationEventType invalidator) {
		this.date = date;
		this.guid = guid;
		this.invalidator = invalidator;
	}
}
