package org.gama.stalactite.sql.binder;

/**
 * Base class to {@link ParameterBinder}s that maps Enum types
 * 
 * @author Guillaume Mary
 */
public abstract class AbstractEnumParameterBinder<E extends Enum<E>> implements ParameterBinder<E> {
	protected final Class<E> enumType;
	
	public AbstractEnumParameterBinder(Class<E> enumType) {
		this.enumType = enumType;
	}
	
	public Class<E> getEnumType() {
		return enumType;
	}
}
