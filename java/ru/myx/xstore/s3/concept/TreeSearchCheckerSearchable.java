/**
 * 
 */
package ru.myx.xstore.s3.concept;

final class TreeSearchCheckerSearchable extends TreeSearchChecker {
	private final boolean	value;
	
	TreeSearchCheckerSearchable(final boolean value) {
		this.value = value;
	}
	
	@Override
	final boolean check(final LinkData subject) {
		return subject.searchable == this.value;
	}
	
	@Override
	public String toString() {
		return "$searchable=" + this.value;
	}
}
