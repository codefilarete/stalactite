package org.gama.stalactite.persistence.id;

/**
 * A decorator for already persisted bean identifier.
 * 
 * @param <T> the real type of the identifier
 * @author Guillaume Mary
 */
public class PersistedIdentifier<T> extends AbstractIdentifier<T> {
	
	public PersistedIdentifier(T surrogate) {
		super(surrogate);
	}
	
	/**
	 * @return true
	 */
	public final boolean isPersisted() {
		return true;
	}
}
