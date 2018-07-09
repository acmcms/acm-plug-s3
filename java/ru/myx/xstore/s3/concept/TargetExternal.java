/**
 * 
 */
package ru.myx.xstore.s3.concept;

import java.io.InputStream;

/**
 * @author myx
 * 
 */
public interface TargetExternal {
	/**
	 * @param identity
	 * @param date
	 * @param type
	 * @param data
	 * @return byte size
	 * @throws Exception
	 */
	int accept(final String identity, final long date, final String type, final InputStream data) throws Exception;
}
