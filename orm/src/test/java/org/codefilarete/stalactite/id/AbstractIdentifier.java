package org.codefilarete.stalactite.id;

import org.codefilarete.tool.Reflections;

/**
 * A decorator for already persisted bean identifier.
 * 
 * @param <T> the real type of the identifier
 * @author Guillaume Mary
 */
public abstract class AbstractIdentifier<T> implements Identifier<T> {
	
	/** Real value, not null */
	private final T delegate;
	
	public AbstractIdentifier(T delegate) {
		this.delegate = delegate;
	}
	
	public T getDelegate() {
		return delegate;
	}
	
	/**
	 * Implementation based on delegate equality.
	 * @param o another Object
	 * @return true if the other object as the same (equally) delegate than this instance
	 */
	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		} else if (o instanceof AbstractIdentifier) {
			AbstractIdentifier<?> that = (AbstractIdentifier<?>) o;
			return equalsDeeply(that);
		} else {
			return false;
		}
	}
	
	/**
	 * To be overridden to add complementary verification
	 * @param that another objet, not null
	 * @return true if this delegate equals the other delegate
	 */
	protected boolean equalsDeeply(AbstractIdentifier<?> that) {
		return delegate.equals(that.delegate);
	}
	
	/**
	 * Implementation based on delegate hashCode.
	 * @return the delegate hashCode
	 */
	@Override
	public int hashCode() {
		return delegate.hashCode();
	}
	
	@Override
	public String toString() {
		return Reflections.toString(getClass()) + "@" + delegate;
	}
}
