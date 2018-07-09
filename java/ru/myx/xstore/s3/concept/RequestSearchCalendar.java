/**
 * 
 */
package ru.myx.xstore.s3.concept;

import java.util.Map;

import ru.myx.ae2.indexing.ExecSearchCalendarInstruction;
import ru.myx.ae2.indexing.ExecSearchCalendarProgram;
import ru.myx.ae2.indexing.IndexingFinder;

/**
 * @author myx
 * 
 */
public final class RequestSearchCalendar {
	/**
	 * 
	 */
	public final IndexingFinder	finder;
	
	private final String		lnkId;
	
	private final boolean		all;
	
	private final long			timeout;
	
	private final long			dateStart;
	
	private final long			dateEnd;
	
	private final String		filter;
	
	/**
	 * @param finder
	 * @param lnkId
	 * @param all
	 * @param timeout
	 * @param dateStart
	 * @param dateEnd
	 * @param filter
	 */
	public RequestSearchCalendar(final IndexingFinder finder,
			final String lnkId,
			final boolean all,
			final long timeout,
			final long dateStart,
			final long dateEnd,
			final String filter) {
		this.finder = finder;
		this.lnkId = lnkId;
		this.all = all;
		this.timeout = timeout;
		this.dateStart = dateStart;
		this.dateEnd = dateEnd;
		this.filter = filter;
	}
	
	/**
	 * @param parent
	 * @return result
	 */
	public final Map<Integer, Map<Integer, Map<String, Number>>> doSearch(final StorageNetwork parent) {
		parent.statIncrementSearchCalendar();
		final ExecSearchCalendarProgram program = this.finder.searchCalendar( this.lnkId,
				this.all,
				this.dateStart,
				this.dateEnd,
				this.filter );
		if (program == null) {
			parent.statIncrementSearchCalendarEmpty();
			return null;
		}
		for (final long timeout = this.timeout <= 0L
				? Long.MAX_VALUE
				: System.currentTimeMillis() + this.timeout; timeout > System.currentTimeMillis();) {
			final ExecSearchCalendarInstruction instruction = program.nextItem();
			if (instruction == null) {
				break;
			}
			parent.statIncrementSearchCalendarItem();
			parent.enqueueTask( instruction );
			instruction.baseValue();
		}
		return program.baseValue();
	}
}
