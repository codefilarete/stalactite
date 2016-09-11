package org.gama.reflection;

/**
 * Marker for unresolvable mutator.
 * 
 * @author Guillaume Mary
 */
public class NotReversibleAccessor extends RuntimeException {
	
	public NotReversibleAccessor(String message) {
		super(message);
	}
}
