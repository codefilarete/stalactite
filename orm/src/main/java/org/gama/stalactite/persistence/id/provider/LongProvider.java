package org.gama.stalactite.persistence.id.provider;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * A simple provider based on long incrementation.
 * 
 * Thread-safe since based on {@link AtomicLong}
 * 
 * @author Guillaume Mary
 */
public class LongProvider extends IdentifierSupplier<Long> {
	
	private static Supplier<Long> wrap(AtomicLong atomicLong) {
		return atomicLong::getAndIncrement;
	}
	
	/**
	 * Basic constructor.
	 * @param initialValue the very first value to be returned by this provider
	 */
	public LongProvider(long initialValue) {
		super(wrap(new AtomicLong(initialValue)));
	}
}
