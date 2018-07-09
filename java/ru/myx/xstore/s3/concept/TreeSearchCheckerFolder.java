/**
 * 
 */
package ru.myx.xstore.s3.concept;

final class TreeSearchCheckerFolder extends TreeSearchChecker {
	private final boolean	value;
	
	TreeSearchCheckerFolder(final boolean value) {
		this.value = value;
	}
	
	@Override
	final boolean check(final LinkData subject) {
		return subject.lnkFolder == this.value;
	}
	
	@Override
	public String toString() {
		return "$folder=" + this.value;
	}
}
