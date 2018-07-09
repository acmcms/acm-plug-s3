/**
 *
 */
package ru.myx.xstore.s3;

import java.util.function.Function;

import ru.myx.ae3.control.command.ControlCommand;

class SignalCommand implements Function<Void, Object> {

	private final StorageLevel3 handler;

	private final ControlCommand<?> command;

	SignalCommand(final StorageLevel3 handler, final ControlCommand<?> command) {

		this.handler = handler;
		this.command = command;
	}

	@Override
	public Object apply(final Void obj) {

		return this.handler.getCommandResult(this.command, null);
	}

	@Override
	public String toString() {

		return this.command.getTitle();
	}
}
