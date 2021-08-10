package org.gama.stalactite.persistence.sql;

import java.sql.SQLException;

import org.gama.lang.Retryer;
import org.gama.lang.exception.Exceptions;

/**
 * @author Guillaume Mary
 */
public class MySQLRetryer extends Retryer {
	
	public MySQLRetryer() {
		this(3, 200);
	}
	
	public MySQLRetryer(int maxRetries, long retryDelay) {
		super(maxRetries, retryDelay);
	}
	
	@Override
	protected boolean shouldRetry(Throwable t) {
		return Exceptions.findExceptionInCauses(t, SQLException.class, "Lock wait timeout exceeded; try restarting transaction") != null;
	}
}
