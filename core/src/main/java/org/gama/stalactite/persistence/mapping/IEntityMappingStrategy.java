package org.gama.stalactite.persistence.mapping;

/**
 * The interface defining methods necessary to persist an entity (ie an object with an id)
 * 
 * @author Guillaume Mary
 */
public interface IEntityMappingStrategy<T, S> extends IMappingStrategy<T> {
	
	S getId(T t);
	
	void setId(T t, S identifier);
}
