package org.gama.stalactite.persistence.id;

import javax.annotation.Nonnull;

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
	
	@Override
	protected boolean equalsDeeply(@Nonnull AbstractIdentifier<?> that) {
		if (super.equalsDeeply(that) && that instanceof PersistedIdentifier) {
			return true;
		} else {
			return false;
		}
	}
}
