/**
 * 
 */
package ru.myx.xstore.s3.concept;

final class TreeSearchCheckerKey extends TreeSearchChecker {
	private final String	value;
	
	TreeSearchCheckerKey(final String value) {
		this.value = value;
	}
	
	@Override
	final boolean check(final LinkData subject) {
		return this.value.equals( subject.lnkName );
	}
	
	@Override
	public String toString() {
		return "$key=" + this.value;
	}
}
