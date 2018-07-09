/**
 *
 */
package ru.myx.xstore.s3;

import ru.myx.xstore.s3.concept.Transaction;

/** @author myx */
interface ChangeNested {

	/** @param transaction
	 * @return boolean
	 * @throws Throwable
	 */
	boolean realCommit(final Transaction transaction) throws Throwable;

}
