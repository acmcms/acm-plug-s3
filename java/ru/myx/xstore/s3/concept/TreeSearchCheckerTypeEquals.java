/**
 * 
 */
package ru.myx.xstore.s3.concept;

final class TreeSearchCheckerTypeEquals extends TreeSearchChecker {
	private final String	value;
	
	TreeSearchCheckerTypeEquals(final String value) {
		this.value = value;
	}
	
	@Override
	final boolean check(final LinkData subject) {
		return this.value.equals( subject.objType );
	}
	
	@Override
	public String toString() {
		return "$type=" + this.value;
	}
}
