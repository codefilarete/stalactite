package org.codefilarete.stalactite.sql;

import java.sql.SQLException;

import org.codefilarete.tool.Retryer;
import org.codefilarete.tool.exception.Exceptions;

/**
 * Retryer for InnoDB false "lock wait timeout exceeded" that appears on heavy load.
 * See https://www.percona.com/blog/2012/03/27/innodbs-gap-locks/ for some more explanation and eventual different workaround.
 * Here is a short description in case the article disappears : initial reason is the way Innodb locks rows while writing : gap lock.
 * 2 workarounds are given by above site:
 * - use {@link java.sql.Connection#TRANSACTION_READ_COMMITTED} because no lock is applied since changes may occur in this mode
 * - apply innodb_locks_unsafe_for_binlog = 1 on server to disable gap lock. Use with extrem caution since it affects all SQL orders
 *
 * @author Guillaume Mary
 */
public class InnoDBLockRetryer extends Retryer {
	
	public InnoDBLockRetryer() {
		this(3, 200);
	}
	
	public InnoDBLockRetryer(int maxRetries, long retryDelay) {
		super(maxRetries, retryDelay);
	}
	
	@Override
	protected boolean shouldRetry(Result result) {
		if (result instanceof Failure<?>) {
			return Exceptions.findExceptionInCauses(((Failure<?>) result).getError(), SQLException.class, "Lock wait timeout exceeded; try restarting transaction") != null;
		} else {
			return false;
		}
	}
}
