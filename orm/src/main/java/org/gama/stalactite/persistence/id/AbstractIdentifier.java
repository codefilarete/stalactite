package org.gama.stalactite.persistence.id;

import javax.annotation.Nonnull;

import org.gama.lang.Reflections;

/**
 * A decorator for already persisted bean identifier.
 * 
 * @param <T> the real type of the identifier
 * @author Guillaume Mary
 */
public abstract class AbstractIdentifier<T> implements Identifier<T> {
	
	/** Real value, not null */
	@Nonnull
	private final T surrogate;
	
	public AbstractIdentifier(@Nonnull T surrogate) {
		this.surrogate = surrogate;
	}
	
	@Nonnull
	public T getSurrogate() {
		return surrogate;
	}
	
	/**
	 * Implementation based on surrogate equality.
	 * @param o another Object
	 * @return true if the other object as the same (equally) surrogate than this instance
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
	 * To be overriden to add complementary verification
	 * @param that another objet, not null, not this
	 * @return true if this surrogate equals the other surrogate
	 */
	protected boolean equalsDeeply(@Nonnull AbstractIdentifier<?> that) {
		return surrogate.equals(that.surrogate);
	}
	
	/**
	 * Implementation based on surrogate hashCode.
	 * @return the surrogate hashCode
	 */
	@Override
	public int hashCode() {
		return surrogate.hashCode();
	}
	
	@Override
	public String toString() {
		return Reflections.toString(getClass()) + "@" + surrogate;
	}
}
