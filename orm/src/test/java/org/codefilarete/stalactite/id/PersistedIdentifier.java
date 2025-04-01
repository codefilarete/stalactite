package org.codefilarete.stalactite.id;

/**
 * A decorator for already persisted bean identifier.
 * 
 * @param <T> the real type of the identifier
 * @author Guillaume Mary
 */
public class PersistedIdentifier<T> extends AbstractIdentifier<T> {
	
	public PersistedIdentifier(T delegate) {
		super (delegate);
	}
	
	/**
	 * @return true
	 */
	public final boolean isPersisted() {
		return true;
	}
	
	@Override
	public void setPersisted() {
		// nothing to do because instances of this class are defacto considered as persisted
	}
}
