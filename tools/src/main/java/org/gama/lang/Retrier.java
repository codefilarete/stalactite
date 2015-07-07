package org.gama.lang;

import org.gama.lang.bean.IDelegateWithResult;

/**
 * @author Guillaume Mary
 */
public abstract class Retrier {

	private int tryCount = 0;
	private final int maxRetries;
	private final long retryDelay;

	public Retrier(int maxRetries, long retryDelay) {
		this.maxRetries = maxRetries;
		this.retryDelay = retryDelay;
	}

	public <T> T execute(IDelegateWithResult<T> delegateWithResult, String description) throws Throwable {
		while(tryCount < maxRetries) {
			try {
				return delegateWithResult.execute();
			} catch (Throwable t) {
				if (shouldRetry(t)) {
					tryCount++;
					waitRetryDelay();
					return execute(delegateWithResult, description);
				} else {
					throw t;
				}
			}
		}
		if (tryCount == maxRetries) {
			throw new RetryException("Action " + description + " was executed " + tryCount + " times every "+ retryDelay + "ms and always failed");
		}
		return null;
	}

	protected abstract boolean shouldRetry(Throwable t);

	private void waitRetryDelay() {
		try {
			Thread.sleep(retryDelay);
		} catch (InterruptedException ignored) {
		}
	}

	public static class RetryException extends Exception {

		public RetryException(String s) {
			super(s);
		}
	}
}
