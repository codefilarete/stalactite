package org.gama.stalactite.persistence.id.provider;

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.gama.stalactite.persistence.id.PersistableIdentifier;

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
	
	private final BlockingQueue<PersistableIdentifier<T>> queue;
	private final int threshold;
	private final Executor executor;
	private final Duration timeOut;
	
	/**
	 * 
	 * @param initialValues the initial values for filling this queue. Can be empty, not null.
	 * @param threshold the threshold below (excluded) which the backgound service is called for filling this queue
	 * @param executor the executor capable of running a background filling of this queue
	 * @param timeOut the time-out after which the {@link #giveNewIdentifier()} gives up and returns null because of an empty queue
	 */
	public PooledIdentifierProvider(Collection<T> initialValues, int threshold, Executor executor, Duration timeOut) {
		// we use LinkedBlockingQueue because it's not bounded (with this constructor) and we need it because of our implementation
		// of giveNewIdentifier that doesn't guarantee the number of elements in the stack
		this.queue = new LinkedBlockingQueue<>(initialValues.stream().map(PersistableIdentifier::new).collect(Collectors.toList()));
		this.threshold = threshold;
		this.executor = executor;
		this.timeOut = timeOut;
	}
	
	/**
	 * Pop the queue by waiting for any value if there's not any more taking into account the timeout.
	 */
	private PersistableIdentifier<T> pop() {
		try {
			return queue.poll(timeOut.getSeconds(), TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			// unreachable
			return null;
		}
	}
	
	/**
	 * Warn: may return null if time-out defined at construction time was reached when trying to pop this queue
	 * @return the identifier on the "top" of this queue
	 */
	@Override
	public PersistableIdentifier<T> giveNewIdentifier() {
		// we ask for a queue filling if we reached the given threshold (below which we must refuel)
		// NB: this is not thread-safe so we may add several tasks of refuel to the executor. I don't think it's a problem because will have some
		// more values reserved. It's not a perfect algorithm and the caveat is on memory consumption.
		// (we do it before pop() to take first call where the queue is empty hence popo() will time out and would return null)
		boolean refueling = ensureMinimalPool();
		PersistableIdentifier<T> toReturn = pop();
		// we do it again for further calls to pop (if not already pending/running)
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
		this.queue.addAll(retrieveSomeValues().stream().map(PersistableIdentifier::new).collect(Collectors.toList()));
	}
	
	/**
	 * Should return an unspecified number of values that will be pooled. Implementations are free to give more or less values depending on what
	 * they want !
	 * @return some values that will be transformed as {@link PersistableIdentifier}
	 */
	protected abstract Collection<T> retrieveSomeValues();
}
