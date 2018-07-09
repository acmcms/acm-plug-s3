/**
 * 
 */
package ru.myx.xstore.s3.concept;

final class TreeSearchCheckerListable extends TreeSearchChecker {
	private final boolean	value;
	
	TreeSearchCheckerListable(final boolean value) {
		this.value = value;
	}
	
	@Override
	final boolean check(final LinkData subject) {
		return subject.listable == this.value;
	}
	
	@Override
	public String toString() {
		return "$listable=" + this.value;
	}
}
