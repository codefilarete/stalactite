package org.gama.lang.trace;

/**
 * A simple incrementable int. Not thread-safe. Prefer {@link java.util.concurrent.atomic.AtomicInteger} for thread safety.
 *
 * @author Guillaume Mary
 */
public class IncrementableInt {
	
	private int value;
	
	public IncrementableInt() {
		this(0);
	}
	
	public IncrementableInt(int value) {
		this.value = value;
	}
	
	public int getValue() {
		return value;
	}
	
	public void increment() {
		value++;
	}
	public void increment(int increment) {
		value += increment;
	}
}