package org.gama.stalactite.persistence.id;

/**
 * An identifier that can have its persisted state changed.
 * To be used for newly created instance that are not yet inserted in database.
 * 
 * @author Guillaume Mary
 */
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
	 * Change the peristed state of this identifier. Expected to be used post-commit.
	 * @param persisted true for persisted state, should never changed after.
	 */
	public void setPersisted(boolean persisted) {
		this.persisted = persisted;
	}
	
	@Override
	protected boolean equals(AbstractIdentifier<?> that) {
		if (super.equals(that) && that instanceof PersistableIdentifier) {
			return this.persisted == ((PersistableIdentifier) that).persisted;
		} else {
			return false;
		}
	}
}
