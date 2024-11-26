/*
 * Created on 09.07.2004
 */
package ru.myx.xstore.s3.local;

import java.util.Map;

import ru.myx.ae3.cache.CacheL2;
import ru.myx.ae3.cache.CreationHandlerObject;
import ru.myx.ae3.report.Report;
import ru.myx.xstore.s3.concept.RequestSearchCalendar;
import ru.myx.xstore.s3.concept.StorageNetwork;

final class CreatorSearchCalendar implements CreationHandlerObject<RequestSearchCalendar, Map<Integer, Map<Integer, Map<String, Number>>>> {
	
	private static final long TTL = 5L * 60_000L;

	private final Map<Integer, Map<Integer, Map<String, Number>>> NULL_OBJECT;

	private final StorageNetwork parent;

	CreatorSearchCalendar(final StorageNetwork parent, final Map<Integer, Map<Integer, Map<String, Number>>> NULL_OBJECT) {
		
		this.parent = parent;
		this.NULL_OBJECT = NULL_OBJECT;
	}

	@Override
	public final Map<Integer, Map<Integer, Map<String, Number>>> create(final RequestSearchCalendar attachment, final String key) {
		
		try {
			final Map<Integer, Map<Integer, Map<String, Number>>> result = attachment.doSearch(this.parent);
			return result == null
				? this.NULL_OBJECT
				: result;
		} catch (final RuntimeException e) {
			throw e;
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public final long getTTL() {
		
		return CreatorSearchCalendar.TTL;
	}

	final Map<Integer, Map<Integer, Map<String, Number>>>
			search(final CacheL2<Object> cacheTree, final String lnkId, final boolean all, final long timeout, final long dateStart, final long dateEnd, final String filter) {
		
		final String key = new StringBuilder().append(lnkId).append('\n').append(all).append('\n').append(timeout).append('\n').append(dateStart).append('\n').append(dateEnd)
				.append('\n').append(filter).toString();
		final Map<Integer, Map<Integer, Map<String, Number>>> check1 = cacheTree.get(lnkId, key);
		if (check1 != null) {
			return check1 == this.NULL_OBJECT
				? null
				: check1;
		}
		final RequestSearchCalendar attachment = new RequestSearchCalendar(this.parent.getFinder(), lnkId, all, timeout, dateStart, dateEnd, filter);
		Report.info("STRLCL", "MISS-SEARCH-CALENDAR: " + lnkId + ", " + all + ", " + timeout + ", " + dateStart + ", " + dateEnd + ", " + filter);
		final Map<Integer, Map<Integer, Map<String, Number>>> check2 = cacheTree.get(lnkId, key, attachment, key, this);
		return check2 == this.NULL_OBJECT
			? null
			: check2;
	}
}
