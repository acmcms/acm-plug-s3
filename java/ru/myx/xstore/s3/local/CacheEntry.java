/**
 * 
 */
package ru.myx.xstore.s3.local;

import java.lang.ref.Reference;
import java.lang.ref.SoftReference;
import java.util.ConcurrentModificationException;
import java.util.Map;
import java.util.TreeMap;

import ru.myx.ae3.extra.External;
import ru.myx.xstore.s3.concept.LinkData;
import ru.myx.xstore.s3.concept.TreeData;

/**
 * @author myx
 * 
 */
final class CacheEntry {
	/**
	 * 
	 */
	public Reference<LinkData>					linkData	= null;
	
	/**
	 * 
	 */
	public Reference<TreeData>					treeData	= null;
	
	/**
	 * 
	 */
	public Reference<External>					extraData	= null;
	
	/**
	 * 
	 */
	public Reference<Map<String, LinkData[]>>	arrays		= null;
	
	/**
	 * @param key
	 * @return listing
	 */
	public final LinkData[] getByKey(final String key) {
		final Reference<Map<String, LinkData[]>> reference = this.arrays;
		if (reference == null) {
			return null;
		}
		final Map<String, LinkData[]> arrays = reference.get();
		if (arrays == null) {
			return null;
		}
		try {
			return arrays.get( key );
		} catch (final ConcurrentModificationException e) {
			Thread.yield();
			synchronized (this) {
				return arrays.get( key );
			}
		}
	}
	
	/**
	 * @param key
	 * @param array
	 */
	public final void setByKey(final String key, final LinkData[] array) {
		synchronized (this) {
			final Reference<Map<String, LinkData[]>> reference = this.arrays;
			final Map<String, LinkData[]> arrays = reference == null
					? null
					: reference.get();
			final Map<String, LinkData[]> target;
			if (arrays == null) {
				target = new TreeMap<>();
				this.arrays = new SoftReference<>( target );
			} else {
				target = arrays;
			}
			target.put( key, array );
		}
	}
}
