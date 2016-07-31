package org.gama.stalactite.persistence.id;

/**
 * A decorator for already persisted bean identifier.
 * 
 * @param <T> the real type of the identifier
 * @author Guillaume Mary
 */
public abstract class AbstractIdentifier<T> implements Identifier<T> {
	
	private final T surrogate;
	
	public AbstractIdentifier(T surrogate) {
		this.surrogate = surrogate;
	}
	
	public T getSurrogate() {
		return surrogate;
	}
	
}
