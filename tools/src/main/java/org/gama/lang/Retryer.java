package org.gama.lang;

import org.gama.lang.bean.IDelegateWithReturnAndThrows;
import org.gama.lang.exception.Exceptions;

/**
 * @author Guillaume Mary
 */
public abstract class Retryer {
	
	public static final Retryer NO_RETRY = new NoRetryer();

	private final int maxRetries;
	private final long retryDelay;

	public Retryer(int maxRetries, long retryDelay) {
		this.maxRetries = maxRetries;
		this.retryDelay = retryDelay;
	}

	public <T> T execute(IDelegateWithReturnAndThrows<T> delegateWithResult, String description) throws Throwable {
		Executor<T> executor = new Executor(delegateWithResult, description);
		return executor.execute();
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
	
	/**
	 * Internal méthode ofr execution in order to make "tryCount" thread-safe
	 * @param <T>
	 */
	private final class Executor<T> {
		private int tryCount = 0;
		private final IDelegateWithReturnAndThrows<T> delegateWithResult;
		private final String description;
		private Executor(IDelegateWithReturnAndThrows<T> delegateWithResult, String description) {
			this.delegateWithResult = delegateWithResult;
			this.description = description;
		}
		public T execute() throws Throwable {
			try {
				tryCount++;
				return delegateWithResult.execute();
			} catch (Throwable t) {
				if (shouldRetry(t)) {
					if (tryCount < maxRetries) {
						waitRetryDelay();
						return execute();
					} else {
						throw new RetryException(description, tryCount, retryDelay, t);
					}
				} else {
					throw t;
				}
			}
		}
	}
	
	private static final class NoRetryer extends Retryer {
		public NoRetryer() {
			super(0, 0);
		}
		
		@Override
		protected boolean shouldRetry(Throwable t) {
			return false;
		}
	}
}
