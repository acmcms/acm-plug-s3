/**
 *
 */
package ru.myx.xstore.s3.local;

import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
import java.util.ConcurrentModificationException;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;

import ru.myx.ae3.act.Act;
import java.util.function.Function;
import ru.myx.ae3.act.ActService;
import ru.myx.ae3.exec.Exec;
import ru.myx.ae3.report.Report;

/**
 * @author myx
 *
 */
public final class CleanerTask implements ActService, Function<ServerLocal, Boolean> {
	
	/**
	 *
	 */
	public static final CleanerTask INSTANCE = new CleanerTask();
	
	private static final Deque<Reference<ServerLocal>> servers = new LinkedList<>();
	
	static {
		Act.launchService(Exec.createProcess(null, "S3-COMMON-CLEANER"), CleanerTask.INSTANCE);
	}
	
	static final void register(final ServerLocal local) {
		
		CleanerTask.servers.addLast(new WeakReference<>(local));
	}
	
	private boolean destroyed = false;
	
	private CleanerTask() {
		// ignore
	}
	
	@Override
	public final Boolean apply(final ServerLocal arg) {
		
		arg.checkOnce();
		return Boolean.TRUE;
	}
	
	@Override
	public boolean main() throws Throwable {
		
		if (!CleanerTask.servers.isEmpty()) {
			{
				final Reference<ServerLocal> reference = CleanerTask.servers.removeFirst();
				final ServerLocal server = reference.get();
				if (server != null) {
					try {
						Act.run( //
								Exec.createProcess(server.getStorage().getServer().getRootContext(), "Cleaner Task (S3)"),
								this,
								server//
						);
					} finally {
						CleanerTask.servers.addLast(reference);
					}
				}
			}
			{
				try {
					for (final Iterator<Reference<ServerLocal>> i = CleanerTask.servers.iterator(); i.hasNext();) {
						final Reference<ServerLocal> reference = i.next();
						final ServerLocal server = reference.get();
						if (server == null) {
							i.remove();
						} else {
							server.checkDiscard();
						}
					}
				} catch (final ConcurrentModificationException e) {
					try {
						Thread.sleep(1000L);
					} catch (final InterruptedException ee) {
						return false;
					}
					return !this.destroyed;
				}
			}
		}
		try {
			Thread.sleep(20000L);
		} catch (final InterruptedException e) {
			return false;
		}
		return !this.destroyed;
	}
	
	@Override
	public final boolean start() {
		
		this.destroyed = false;
		return true;
	}
	
	@Override
	public final boolean stop() {
		
		this.destroyed = true;
		return false;
	}
	
	@Override
	public String toString() {
		
		return "System-wide xstore.s3 local/private cache cleaner task";
	}
	
	@Override
	public final boolean unhandledException(final Throwable t) {
		
		Report.exception("SERVICE:S3-LCL-CLEANER-TASK", "unhandled", t);
		return !this.destroyed;
	}
	
}
