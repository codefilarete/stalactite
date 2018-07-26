package org.gama.stalactite.persistence.id;

import javax.annotation.Nonnull;

import org.gama.stalactite.persistence.id.diff.IdentifiedCollectionDiffer;

/**
 * An identifier that can have its persisted state changed.
 * To be used for newly created instance that are not yet inserted in database.
 * 
 * @author Guillaume Mary
 */
@SuppressWarnings("squid:S2160")	// no need to override "equals" since it already delegates to the overridable method "equalsDeeply"
public class PersistableIdentifier<T> extends AbstractIdentifier<T> {
	
	private boolean persisted = false;
	
	public PersistableIdentifier(T surrogate) {
		super(surrogate);
	}
	
	@Override
	public boolean isPersisted() {
		return persisted;
	}
	
	/**
	 * Changes the peristed state of this identifier. Expected to be used post-commit.
	 * @param persisted true for persisted state, should never changed after.
	 */
	public void setPersisted(boolean persisted) {
		this.persisted = persisted;
	}
	
	/**
	 * Overriden to be compatible with {@link PersistedIdentifier}.
	 * This is particularly usefull with {@link IdentifiedCollectionDiffer} because it may compare persisted and not persisted identifier.
	 * 
	 * @param that another objet, not null, not this
	 * @return true if persisted state are equal, or if this instance is persisted and {@code that} is a {@link PersistedIdentifier}
	 */
	@Override
	protected boolean equalsDeeply(@Nonnull AbstractIdentifier<?> that) {
		if (super.equalsDeeply(that)) {
			if (that instanceof PersistableIdentifier) {
				return this.persisted == ((PersistableIdentifier) that).persisted;
			} else if (that instanceof PersistedIdentifier) {
				return this.persisted;
			} else {
				return false;
			}
		} else {
			return false;
		}
	}
}
