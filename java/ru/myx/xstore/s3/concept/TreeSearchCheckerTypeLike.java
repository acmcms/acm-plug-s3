/**
 * 
 */
package ru.myx.xstore.s3.concept;

final class TreeSearchCheckerTypeLike extends TreeSearchChecker {
	private final String	value;
	
	private final String	valueUpperCase;
	
	private final int		length;
	
	TreeSearchCheckerTypeLike(final String value) {
		this.value = value;
		this.valueUpperCase = value.toUpperCase();
		this.length = value.length();
	}
	
	@Override
	final boolean check(final LinkData subject) {
		final String type = subject.objType;
		if (type == null || type.length() < this.length) {
			return false;
		}
		return type.contains( this.value ) || type.toUpperCase().contains( this.valueUpperCase );
	}
	
	@Override
	public String toString() {
		return "$type:" + this.value;
	}
}
