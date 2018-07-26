package org.gama.stalactite.persistence.id;

import javax.annotation.Nonnull;

import org.gama.stalactite.persistence.id.diff.IdentifiedCollectionDiffer;

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
	
	/**
	 * Overriden to be compatible with {@link PersistableIdentifier}.
	 * This is particularly usefull with {@link IdentifiedCollectionDiffer} because it may compare persisted and not persisted identifier.
	 * 
	 * @param that another objet, not null, not this
	 * @return true if {@code that} is a {@link PersistedIdentifier}, or {@code that} is a {@link PersistableIdentifier} and its state is persisted 
	 */
	@Override
	protected boolean equalsDeeply(@Nonnull AbstractIdentifier<?> that) {
		if (super.equalsDeeply(that)) {
			if (that instanceof PersistedIdentifier) {
				return true;
			} else if (that instanceof PersistableIdentifier) {
				return that.isPersisted();
			} else {
				return false;
			}
		} else {
			return false;
		}
	}
}
