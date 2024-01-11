/**
 *
 */
package ru.myx.xstore.s3;

import ru.myx.xstore.s3.concept.Transaction;

/** @author myx */
final class ChangeDoUnlink implements ChangeNested {
	
	private final EntryImpl entry;
	
	private final boolean soft;
	
	/** @param entry
	 * @param soft */
	ChangeDoUnlink(final EntryImpl entry, final boolean soft) {
		
		this.entry = entry;
		this.soft = soft;
	}
	
	@Override
	public boolean realCommit(final Transaction transaction) throws Throwable {
		
		this.entry.getType().onBeforeDelete(this.entry);
		transaction.unlink(this.entry.getLink(), this.soft);
		return true;
	}
	
}
