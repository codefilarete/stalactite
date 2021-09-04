package org.gama.stalactite.persistence.sql;

import java.sql.SQLException;

import org.gama.lang.Retryer;
import org.gama.lang.exception.Exceptions;

/**
 * @author Guillaume Mary
 */
public class MariaDBRetryer extends Retryer {
	
	public MariaDBRetryer() {
		this(3, 200);
	}
	
	public MariaDBRetryer(int maxRetries, long retryDelay) {
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
