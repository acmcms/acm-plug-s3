package ru.myx.xstore.s3.concept;

abstract class TreeSearchCallback {
	/**
	 * @param link
	 * @return true to stop!
	 */
	abstract boolean execute(final LinkData link);
}
