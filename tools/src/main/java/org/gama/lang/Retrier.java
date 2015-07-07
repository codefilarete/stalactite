package org.gama.lang;

import org.gama.lang.bean.IDelegateWithReturnAndThrows;
import org.gama.lang.exception.Exceptions;

/**
 * @author Guillaume Mary
 */
public abstract class Retrier {
	
	public static final Retrier NO_RETRY = new Retrier(0, 0) {
		@Override
		protected boolean shouldRetry(Throwable t) {
			return false;
		}
	};

	private int tryCount = 0;
	private final int maxRetries;
	private final long retryDelay;

	public Retrier(int maxRetries, long retryDelay) {
		this.maxRetries = maxRetries;
		this.retryDelay = retryDelay;
	}

	public <T> T execute(IDelegateWithReturnAndThrows<T> delegateWithResult, String description) throws Throwable {
		try {
			tryCount++;
			return delegateWithResult.execute();
		} catch (Throwable t) {
			if (shouldRetry(t)) {
				if (tryCount < maxRetries) {
					waitRetryDelay();
					return execute(delegateWithResult, description);
				} else {
					throw new RetryException(description, tryCount, retryDelay, t);
				}
			} else {
				throw t;
			}
		}
	}

	protected abstract boolean shouldRetry(Throwable t);

	private void waitRetryDelay() {
		try {
			Thread.sleep(retryDelay);
		} catch (InterruptedException ie) {
			Exceptions.throwAsRuntimeException(ie);
		}
	}

	public static class RetryException extends Exception {

		public RetryException(String action, int tryCount,  long retryDelay, Throwable cause) {
			super("Action \"" + action + "\" has been executed " + tryCount + " times every " + retryDelay + "ms and always failed", cause);
		}
	}
}
