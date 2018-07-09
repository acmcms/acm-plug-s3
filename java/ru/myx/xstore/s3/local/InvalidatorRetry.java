/**
 * 
 */
package ru.myx.xstore.s3.local;

import java.io.File;

import ru.myx.ae3.act.Act;
import ru.myx.ae3.help.Format;
import ru.myx.ae3.report.Report;

final class InvalidatorRetry implements Runnable {
	private final File	target;
	
	private long		delay;
	
	InvalidatorRetry(final File target) {
		this.target = target;
		this.delay = 500L;
	}
	
	@Override
	public final void run() {
		if (this.target.delete() || !this.target.exists()) {
			return;
		}
		this.delay *= 2;
		if (this.delay <= 32000L) {
			Report.info( "S3-INV-RETRY", "Extra invalidate fail - will retry again: guid="
					+ this.target.getName()
					+ ", life="
					+ Format.Compact.toPeriod( System.currentTimeMillis() - this.target.lastModified() )
					+ ", size="
					+ Format.Compact.toBytes( this.target.length() ) );
			Act.later( null, this, this.delay );
		}
	}
	
	@Override
	public final String toString() {
		return "InvalidatorRetry{file=" + this.target.getAbsolutePath() + "}";
	}
}
