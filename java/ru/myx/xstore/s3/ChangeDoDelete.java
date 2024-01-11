/**
 *
 */
package ru.myx.xstore.s3;

import ru.myx.xstore.s3.concept.Transaction;

/** @author myx */
final class ChangeDoDelete implements ChangeNested {
	
	private final EntryImpl entry;
	
	private final boolean soft;
	
	/** @param entry
	 * @param soft */
	ChangeDoDelete(final EntryImpl entry, final boolean soft) {
		
		this.entry = entry;
		this.soft = soft;
	}
	
	@Override
	public boolean realCommit(final Transaction transaction) throws Throwable {
		
		try {
			this.entry.getType().onBeforeDelete(this.entry);
		} catch (final Throwable t) {
			if (!this.soft) {
				throw t;
			}
		}
		transaction.delete(this.entry.getLink(), this.soft);
		return true;
	}
	
}
