package org.codefilarete.stalactite.persistence.engine.idprovider;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Simple provider that maintains a set of identifiers to prevent from a time consuming creation of those identifiers.
 * For instance, used for database based generated identifiers since the retrieval of a bunch of values may be time consuming.
 * Thought for table-sequence-generated identifiers like hi-lo ones.
 * 
 * Thread-safe because based on a {@link BlockingQueue}
 * 
 * @author Guillaume Mary
 */
public abstract class PooledIdentifierProvider<T> implements IdentifierProvider<T> {
	
	private final BlockingQueue<T> queue;
	private final int threshold;
	private final Executor executor;
	private final Duration timeOut;
	
	/**
	 * 
	 * @param initialValues the initial values to fill this queue. Can be empty, not null.
	 * @param threshold the threshold below (excluded) which the background service is called to fill this queue
	 * @param executor the executor capable of running a background task of filling this queue
	 * @param timeOut the timeout after which the {@link #giveNewIdentifier()} gives up and returns null because of an empty queue
	 */
	public PooledIdentifierProvider(Collection<T> initialValues, int threshold, Executor executor, Duration timeOut) {
		// we use LinkedBlockingQueue because it's not bounded (with this constructor) : we need it because our implementation
		// of giveNewIdentifier doesn't guarantee the number of elements in the stack
		this.queue = initialValues.stream().collect(Collectors.toCollection(LinkedBlockingQueue::new));
		this.threshold = threshold;
		this.executor = executor;
		this.timeOut = timeOut;
	}
	
	/**
	 * Pops the queue. If there's not any more it will wait for any value until timeout (defined at construction time).
	 */
	private T pop() {
		try {
			return queue.poll(timeOut.getSeconds(), TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			// unreachable
			return null;
		}
	}
	
	/**
	 * Warn: may return null if timeout (defined at construction time) is reached when trying to pop this queue
	 * @return the identifier on the "top" of this queue
	 */
	@Override
	public T giveNewIdentifier() {
		// We ask for a queue filling if we reached the given threshold (below which we must refuel)
		// Since this code block is not synchronized, we could have several refueling tasks added to the executor. I don't think it's a problem
		// because the consequence is that we'll get some "extra" values reserved : the drawback is memory consumption due to extra identifiers
		// which should be very small, and of course a extra values can be lost if JVM crashes. 
		// (we do it before pop() to take account of very first call : queue can be empty)
		boolean refueling = ensureMinimalPool();
		T toReturn = pop();
		// we do it again for future calls to pop (if not previously done)
		if (!refueling) {
			ensureMinimalPool();
		}
		return toReturn;
	}
	
	private boolean ensureMinimalPool() {
		if (queue.size() < threshold) {
			executor.execute(this::fillQueue);
			return true;
		} else {
			return false;
		}
	}
	
	private void fillQueue() {
		this.queue.addAll(new ArrayList<>(retrieveSomeValues()));
	}
	
	/**
	 * Should return an unspecified number of values that will be pooled. Implementations are free to give more or less values depending on what
	 * they want !
	 * @return some non null values
	 */
	protected abstract Collection<T> retrieveSomeValues();
}
