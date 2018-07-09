/*
 * Created on 09.07.2004
 */
package ru.myx.xstore.s3.local;

import java.util.Map;

import ru.myx.ae3.cache.CacheL2;
import ru.myx.ae3.cache.CreationHandlerObject;
import ru.myx.ae3.report.Report;
import ru.myx.xstore.s3.concept.RequestSearch;
import ru.myx.xstore.s3.concept.StorageNetwork;

final class CreatorSearch implements CreationHandlerObject<RequestSearch, Map.Entry<String, Object>[]> {
	private static final long					TTL	= 5L * 1000L * 60L;
	
	private final Map.Entry<String, Object>[]	NULL_OBJECT;
	
	private final StorageNetwork				parent;
	
	CreatorSearch(final StorageNetwork parent, final Map.Entry<String, Object>[] NULL_OBJECT) {
		this.parent = parent;
		this.NULL_OBJECT = NULL_OBJECT;
	}
	
	@Override
	public final Map.Entry<String, Object>[] create(final RequestSearch attachment, final String key) {
		try {
			final Map.Entry<String, Object>[] result = attachment.doSearch( this.parent );
			return result == null
					? this.NULL_OBJECT
					: result;
		} catch (final RuntimeException e) {
			throw e;
		} catch (final Exception e) {
			throw new RuntimeException( e );
		}
	}
	
	@Override
	public final long getTTL() {
		return CreatorSearch.TTL;
	}
	
	final Map.Entry<String, Object>[] search(
			final CacheL2<Object> cacheTree,
			final String lnkId,
			final int limit,
			final boolean all,
			final long timeout,
			final String sort,
			final long dateStart,
			final long dateEnd,
			final String filter) {
		final String key = new StringBuilder().append( lnkId ).append( '\n' ).append( limit ).append( '\n' )
				.append( all ).append( '\n' ).append( timeout ).append( '\n' ).append( sort ).append( '\n' )
				.append( dateStart ).append( '\n' ).append( dateEnd ).append( '\n' ).append( filter ).toString();
		final Map.Entry<String, Object>[] check1 = cacheTree.get( lnkId, key );
		if (check1 != null) {
			return check1 == this.NULL_OBJECT
					? null
					: check1;
		}
		final RequestSearch attachment = new RequestSearch( this.parent.getFinder(),
				lnkId,
				limit,
				all,
				timeout,
				sort,
				dateStart,
				dateEnd,
				filter );
		Report.info( "STRLCL", "MISS-SEARCH: "
				+ lnkId
				+ ", "
				+ limit
				+ ", "
				+ all
				+ ", "
				+ timeout
				+ ", "
				+ sort
				+ ", "
				+ dateStart
				+ ", "
				+ dateEnd
				+ ", "
				+ filter );
		final Map.Entry<String, Object>[] check2 = cacheTree.get( lnkId, key, attachment, key, this );
		return check2 == this.NULL_OBJECT
				? null
				: check2;
	}
}
