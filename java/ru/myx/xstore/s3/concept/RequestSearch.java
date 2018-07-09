/**
 * 
 */
package ru.myx.xstore.s3.concept;

import java.util.HashMap;
import java.util.Map;

import ru.myx.ae2.indexing.ExecSearchInstruction;
import ru.myx.ae2.indexing.ExecSearchProgram;
import ru.myx.ae2.indexing.IndexingFinder;
import ru.myx.ae3.Engine;
import ru.myx.ae3.help.Convert;

/**
 * @author myx
 * 
 */
public final class RequestSearch {
	private static final Map<String, String>	REPLACEMENT_SEARCH_SORT;
	
	static {
		final Map<String, String> result = new HashMap<>();
		result.put( "alphabet", "$title" );
		result.put( "history", "$created-" );
		result.put( "log", "$created" );
		REPLACEMENT_SEARCH_SORT = result;
	}
	
	/**
	 * 
	 */
	public final IndexingFinder					finder;
	
	private final String						lnkId;
	
	private final int							limit;
	
	private final boolean						all;
	
	private final long							timeout;
	
	private final String						sort;
	
	private final long							dateStart;
	
	private final long							dateEnd;
	
	private final String						filter;
	
	/**
	 * @param finder
	 * @param lnkId
	 * @param limit
	 * @param all
	 * @param timeout
	 * @param sort
	 * @param dateStart
	 * @param dateEnd
	 * @param filter
	 */
	public RequestSearch(final IndexingFinder finder,
			final String lnkId,
			final int limit,
			final boolean all,
			final long timeout,
			final String sort,
			final long dateStart,
			final long dateEnd,
			final String filter) {
		this.finder = finder;
		this.lnkId = lnkId;
		this.limit = limit;
		this.all = all;
		this.timeout = timeout;
		final String sortReplace = sort == null
				? null
				: RequestSearch.REPLACEMENT_SEARCH_SORT.get( sort );
		this.sort = sortReplace == null
				? sort
				: sortReplace;
		this.dateStart = dateStart;
		this.dateEnd = dateEnd;
		this.filter = filter;
	}
	
	/**
	 * @param parent
	 * @return result
	 */
	public final Map.Entry<String, Object>[] doSearch(final StorageNetwork parent) {
		parent.statIncrementSearch();
		final ExecSearchProgram program = this.finder.search( this.lnkId,
				this.all,
				this.sort,
				this.dateStart,
				this.dateEnd,
				this.filter );
		if (program == null) {
			parent.statIncrementSearchEmpty();
			return null;
		}
		for (final long timeoutDate = this.timeout <= 0L
				? Long.MAX_VALUE
				: Engine.fastTime() + this.timeout;;) {
			final ExecSearchInstruction instruction = program.nextItem();
			if (instruction == null) {
				/**
				 * end of instruction queue
				 */
				break;
			}
			if (timeoutDate < Engine.fastTime()) {
				parent.statIncrementSearchTimeout();
				break;
			}
			parent.statIncrementSearchItem();
			parent.enqueueTask( instruction );
			instruction.baseValue();
		}
		final Map.Entry<String, Object>[] result = program.baseValue();
		if (this.limit > 0 && result != null && result.length > this.limit) {
			final Map.Entry<String, Object>[] limited = Convert.Array.toAny( new Map.Entry[this.limit] );
			System.arraycopy( result, 0, limited, 0, this.limit );
			return limited;
		}
		return result;
	}
}
