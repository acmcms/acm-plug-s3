/**
 * 
 */
package ru.myx.xstore.s3.concept;

final class TreeSearchCheckerState extends TreeSearchChecker {
	private final int	value;
	
	TreeSearchCheckerState(final int value) {
		this.value = value;
	}
	
	@Override
	final boolean check(final LinkData subject) {
		return subject.objState == this.value;
	}
	
	@Override
	public String toString() {
		return "$state=" + this.value;
	}
}
