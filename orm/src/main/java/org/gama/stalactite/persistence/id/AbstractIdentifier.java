package org.gama.stalactite.persistence.id;

/**
 * A decorator for already persisted bean identifier.
 * 
 * @param <T> the real type of the identifier
 * @author Guillaume Mary
 */
public abstract class AbstractIdentifier<T> implements Identifier<T> {
	
	/** Real value, not null */
	private final T surrogate;
	
	public AbstractIdentifier(T surrogate) {
		this.surrogate = surrogate;
	}
	
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
			return equals(that);
		} else {
			return false;
		}
	}
	
	/**
	 * To be overriden to add complementary verification
	 * @param that another objet, not null, not this
	 * @return true if this surrogate equals the other surrogate
	 */
	protected boolean equals(AbstractIdentifier<?> that) {
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
}
