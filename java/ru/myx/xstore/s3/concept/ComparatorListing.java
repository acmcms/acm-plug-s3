/*
 * Created on 14.01.2005
 */
package ru.myx.xstore.s3.concept;

import java.util.Comparator;

final class ComparatorListing implements Comparator<LinkData> {
	@Override
	public final int compare(final LinkData arg0, final LinkData arg1) {
		return arg0.lnkName.compareTo( arg1.lnkName );
	}
}
