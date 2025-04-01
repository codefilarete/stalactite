package org.codefilarete.stalactite.id;

/**
 * An identifier that can have its persisted state changed.
 * To be used for newly created instance that are not yet inserted in database.
 * 
 * @author Guillaume Mary
 */
@SuppressWarnings("squid:S2160")	// no need to override "equals" since it already delegates to the overridable method "equalsDeeply"
public class PersistableIdentifier<T> extends AbstractIdentifier<T> {
	
	private boolean persisted = false;
	
	public PersistableIdentifier(T delegate) {
		super (delegate);
	}
	
	@Override
	public boolean isPersisted() {
		return persisted;
	}
	
	/**
	 * Changes the persisted state of this identifier. Expected to be used post-commit.
	 */
	@Override
	public void setPersisted() {
		this.persisted = true;
	}
}
