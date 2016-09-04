package org.gama.stalactite.persistence.mapping;

/**
 * Interface for general access to the identifier of an entity
 * 
 * @author Guillaume Mary
 */
public interface IIdAccessor<T, I> {
	
	I getId(T t);
	
	void setId(T t, I identifier);
}
