/**
 *
 */
package ru.myx.xstore.s3.jdbc;

import ru.myx.jdbc.queueing.RequestAttachment;
import ru.myx.jdbc.queueing.RunnerDatabaseRequestor;
import ru.myx.xstore.s3.concept.TargetExternal;

/** @author myx */
public final class RequestLoadExternal extends RequestAttachment<Boolean, RunnerDatabaseRequestor> {

	private final ServerJdbc server;

	private final String key;

	private final TargetExternal target;

	/** @param server
	 * @param guid
	 * @param target
	 */
	public RequestLoadExternal(final ServerJdbc server, final String guid, final TargetExternal target) {

		this.server = server;
		this.key = guid;
		this.target = target;
	}

	@Override
	public final Boolean apply(final RunnerDatabaseRequestor ctx) {

		try {
			final Boolean result = MatExtra.materialize(this.server, ctx.ctxGetConnection(), this.key, this.target)
				? Boolean.TRUE
				: Boolean.FALSE;
			this.setResult(result);
			return result;
		} catch (final Exception e) {
			throw new RuntimeException(this.getClass().getSimpleName(), e);
		}
	}

	@Override
	public final String getKey() {

		return "le-" + this.key;
	}
}
