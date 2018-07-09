/*
 * Created on 14.01.2005
 */
package ru.myx.xstore.s3.concept;

import java.util.Comparator;

final class ComparatorLog implements Comparator<LinkData> {
	@Override
	public final int compare(final LinkData arg0, final LinkData arg1) {
		final long c0 = arg1.objCreated;
		final long c1 = arg0.objCreated;
		return c0 < c1
				? 1
				: c0 == c1
						? 0
						: -1;
	}
}
